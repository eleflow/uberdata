/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import eleflow.uberdata.core.IUberdataContext;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.exec.*;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.r.ZeppelinRBackend;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SparkRInterpreter extends Interpreter implements ExecuteResultHandler {
    Logger logger = LoggerFactory.getLogger(SparkRInterpreter.class);
    private DefaultExecutor executor;
    private ByteArrayOutputStream outputStream;
    private BufferedWriter ins;
    private PipedInputStream in;
    private ByteArrayOutputStream input;
    private String scriptPath;
    boolean rScriptRunning = false;

    static {
        Interpreter.register(
                "r",
                "iuberdata",
                SparkRInterpreter.class.getName(),
                new InterpreterPropertyBuilder()
                        .add("spark.home",
                                SystemDefaultUtil.getSystemDefault("SPARK_HOME", "spark.home", "/opt/spark"),
                                "Spark home path. Should be provided for sparkR")
                        .add("zeppelin.sparkr.r",
                                SystemDefaultUtil.getSystemDefault("SPARKR_DRIVER_R", "zeppelin.sparkr.r", "Rscript"),
                                "R command to run sparkR with").build());
    }

    volatile private int sparkRBackendPort = 0;

    public SparkRInterpreter(Properties property) {
        super(property);

        scriptPath = System.getProperty("java.io.tmpdir") + "/zeppelin_r.R";
    }

    private String getSparkHome() {
        String sparkHome = getProperty("spark.home");
        if (sparkHome == null) {
            throw new InterpreterException("spark.home is undefined");
        } else {
            return sparkHome;
        }
    }


    private void createRScript() {
        ClassLoader classLoader = getClass().getClassLoader();
        File out = new File(scriptPath);

        if (out.exists() && out.isDirectory()) {
            throw new InterpreterException("Can't create R script " + out.getAbsolutePath());
        }

        try {
            FileOutputStream outStream = new FileOutputStream(out);
            IOUtils.copy(
                    classLoader.getResourceAsStream("R/zeppelin_r.R"),
                    outStream);
            outStream.close();
        } catch (IOException e) {
            throw new InterpreterException(e);
        }

        logger.info("File {} created", scriptPath);
    }

    @Override
    public void open() {
        // create R script
        createRScript();

        int backendTimeout = Integer.parseInt(System.getenv().getOrDefault("SPARKR_BACKEND_TIMEOUT", "120"));

        // Launch a SparkR backend server for the R process to connect to; this will let it see our
        // Java system properties etc.
        ZeppelinRBackend sparkRBackend = new ZeppelinRBackend();

        Semaphore initialized = new Semaphore(0);
        Thread sparkRBackendThread = new Thread("SparkR backend") {
            @Override
            public void run() {
                sparkRBackendPort = sparkRBackend.init();
                initialized.release();
                sparkRBackend.run();
            }
        };

        sparkRBackendThread.start();

        // Wait for RBackend initialization to finish
        try {
            if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
                // Launch R
                CommandLine cmd = CommandLine.parse(getProperty("zeppelin.sparkr.r"));
                cmd.addArgument(scriptPath, false);
                cmd.addArgument("--no-save", false);
                //      cmd.addArgument(getJavaSparkContext().version(), false);
                executor = new DefaultExecutor();
                outputStream = new ByteArrayOutputStream();
                PipedOutputStream ps = new PipedOutputStream();
                in = null;
                try {
                    in = new PipedInputStream(ps);
                } catch (IOException e1) {
                    throw new InterpreterException(e1);
                }
                ins = new BufferedWriter(new OutputStreamWriter(ps));

                input = new ByteArrayOutputStream();

                PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, outputStream, in);
                executor.setStreamHandler(streamHandler);
                executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));


                Map env = EnvironmentUtils.getProcEnvironment();

                String sparkRInterpreterObjId = sparkRBackend.put(this);
                String uberdataContextObjId = sparkRBackend.put(getUberdataContext());
                env.put("R_PROFILE_USER", scriptPath);
                env.put("SPARK_HOME", getSparkHome());
                env.put("EXISTING_SPARKR_BACKEND_PORT", String.valueOf(sparkRBackendPort));
                env.put("SPARKR_INTERPRETER_ID", sparkRInterpreterObjId);
                env.put("UBERDATA_CONTEXT_ID", uberdataContextObjId);
                logger.info("executing {} {}", env, cmd.toString());
                executor.execute(cmd, env, this);
                logger.info("executed");
                rScriptRunning = true;


            } else {
                System.err.println("SparkR backend did not initialize in " + backendTimeout + " seconds");
                System.exit(-1);
            }
        } catch (InterruptedException e) {
            new InterpreterException((e));
        } catch (IOException e) {
            new InterpreterException((e));
        }

    }

    private int findRandomOpenPortOnAllLocalInterfaces() {
        int port;
        try (ServerSocket socket = new ServerSocket(0);) {
            port = socket.getLocalPort();
            socket.close();
        } catch (IOException e) {
            throw new InterpreterException(e);
        }
        return port;
    }

    @Override
    public void close() {
        executor.getWatchdog().destroyProcess();
    }

    RInterpretRequest rInterpretRequest = null;

    /**
     *
     */
    public class RInterpretRequest {
        public String statements;
        public String jobGroup;

        public RInterpretRequest(String statements, String jobGroup) {
            this.statements = statements;
            this.jobGroup = jobGroup;
        }

        public String statements() {
            return statements;
        }

        public String jobGroup() {
            return jobGroup;
        }
    }

    Integer statementSetNotifier = new Integer(0);

    public RInterpretRequest getStatements() {
        synchronized (statementSetNotifier) {
            while (rInterpretRequest == null) {
                try {
                    statementSetNotifier.wait(1000);
                } catch (InterruptedException e) {
                }
            }
            RInterpretRequest req = rInterpretRequest;
            rInterpretRequest = null;
            return req;
        }
    }

    String statementOutput = null;
    boolean statementError = false;
    Integer statementFinishedNotifier = new Integer(0);

    public void setStatementsFinished(String out, boolean error) {
        synchronized (statementFinishedNotifier) {
            statementOutput = out;
            statementError = error;
            statementFinishedNotifier.notify();
        }

    }

    boolean rScriptInitialized = false;
    Integer rScriptInitializeNotifier = new Integer(0);

    public void onRScriptInitialized() {
        synchronized (rScriptInitializeNotifier) {
            rScriptInitialized = true;
            rScriptInitializeNotifier.notifyAll();
        }
    }

    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) {
        if (!rScriptRunning) {
            return new InterpreterResult(Code.ERROR, "R process not running"
                    + outputStream.toString());
        }
        logger.info("R output {}", outputStream.toString());
        outputStream.reset();

        synchronized (rScriptInitializeNotifier) {
            long startTime = System.currentTimeMillis();
            while (rScriptInitialized == false
                    && rScriptRunning
                    && System.currentTimeMillis() - startTime < 10 * 1000) {
                try {
                    rScriptInitializeNotifier.wait(1000);
                } catch (InterruptedException e) {
                }
            }
        }

        if (rScriptRunning == false) {
            // R script failed to initialize and terminated
            return new InterpreterResult(Code.ERROR, "failed to start sparkR"
                    + outputStream.toString());
        }
        if (rScriptInitialized == false) {
            // timeout. didn't get initialized message
            return new InterpreterResult(Code.ERROR, "sparkR is not responding "
                    + outputStream.toString());
        }

        IUberSparkInterpreter sparkInterpreter = getSparkInterpreter();
//    if (!sparkInterpreter.getSparkContext().version().startsWith("1.2") &&
//        !sparkInterpreter.getSparkContext().version().startsWith("1.3")) {
//      return new InterpreterResult(Code.ERROR, "sparkR "
//          + sparkInterpreter.getSparkContext().version() + " is not supported");
//    }
        String jobGroup = sparkInterpreter.getJobGroup(context);
//    ZeppelinContext z = sparkInterpreter.getZeppelinContext();
//    z.setInterpreterContext(context);
//    z.setGui(context.getGui());
        rInterpretRequest = new RInterpretRequest(st, jobGroup);
        statementOutput = null;

        synchronized (statementSetNotifier) {
            statementSetNotifier.notify();
        }

        synchronized (statementFinishedNotifier) {
            while (statementOutput == null) {
                try {
                    statementFinishedNotifier.wait(1000);
                } catch (InterruptedException e) {
                }
            }
        }
        logger.info("R output {}", outputStream.toString());

        if (statementError) {
            return new InterpreterResult(Code.ERROR, statementOutput);
        } else {
            return new InterpreterResult(Code.SUCCESS, statementOutput);
        }
    }

    @Override
    public void cancel(InterpreterContext context) {
        IUberSparkInterpreter sparkInterpreter = getSparkInterpreter();
        sparkInterpreter.cancel(context);
    }

    @Override
    public FormType getFormType() {
        return FormType.NATIVE;
    }

    @Override
    public int getProgress(InterpreterContext context) {
        IUberSparkInterpreter sparkInterpreter = getSparkInterpreter();
        return sparkInterpreter.getProgress(context);
    }

    /*@Override
    public List<String> completion(String buf, int cursor) {
        // not supported
        return new LinkedList<String>();
    }*/

    private IUberSparkInterpreter getSparkInterpreter() {
        InterpreterGroup intpGroup = getInterpreterGroup();

        Interpreter intp = getInterpreterInTheSameSessionByClassName(IUberSparkInterpreter.class.getName());
        if (intp == null) return null;

        Interpreter p = intp;
        while (p instanceof WrappedInterpreter) {
            if (p instanceof LazyOpenInterpreter) {
                ((LazyOpenInterpreter) p).open();
            }
            p = ((WrappedInterpreter) p).getInnerInterpreter();
        }
        return (IUberSparkInterpreter) p;

    }


    public ZeppelinContext getZeppelinContext() {
        IUberSparkInterpreter sparkIntp = getSparkInterpreter();
        if (sparkIntp != null) {
            return getSparkInterpreter().getZeppelinContext();
        } else {
            return null;
        }
    }

    public IUberdataContext getUberdataContext() {
        IUberSparkInterpreter intp = getSparkInterpreter();
        if (intp == null) {
            return null;
        } else {
            return intp.getUberdataContext();
        }
    }


    public JavaSparkContext getJavaSparkContext() {
        IUberSparkInterpreter intp = getSparkInterpreter();
        if (intp == null) {
            return null;
        } else {
            return new JavaSparkContext(intp.getSparkContext());
        }
    }

    public SparkConf getSparkConf() {
        JavaSparkContext sc = getJavaSparkContext();
        if (sc == null) {
            return null;
        } else {
            return getJavaSparkContext().getConf();
        }
    }

    public SQLContext getSQLContext() {
        IUberSparkInterpreter intp = getSparkInterpreter();
        if (intp == null) {
            return null;
        } else {
            return intp.getSQLContext();
        }
    }


    @Override
    public void onProcessComplete(int exitValue) {
        rScriptRunning = false;
        logger.info("R process terminated. exit code " + exitValue);
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
        rScriptRunning = false;
        logger.error("R process failed", e);
//        intp
    }
}
