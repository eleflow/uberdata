//package org.apache.zeppelin.spark;

//import org.apache.commons.exec.*;
//import org.apache.zeppelin.interpreter.InterpreterContext;
//import org.apache.zeppelin.interpreter.InterpreterResult;
//import org.apache.zeppelin.shell.ShellInterpreter;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.IOException;
//import java.util.Properties;

/**
 * Created by dirceu on 23/10/15.
 */
//public class IUberdataShellInterpreter extends ShellInterpreter {
//
//    Logger logger = LoggerFactory.getLogger(IUberdataShellInterpreter .class);
////    static {
////        Logger logger = LoggerFactory.getLogger(IUberdataShellInterpreter .class);
////            register("git", IUberdataShellInterpreter.class.getName());
////    }
//
//    public IUberdataShellInterpreter(Properties properties) {
//        super(properties);
//    }
//
//    private Logger log = LoggerFactory.getLogger(IUberdataShellInterpreter.class);
//
//    private int commandTOut = 600000;
//
//    public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
//        log.debug("Run shell command '" + cmd + "'");
//         System.out.println("aaaa");
//        cmd = "git "+cmd;
//        CommandLine cmdLine = CommandLine.parse("bash");
//        cmdLine.addArgument("-c", false);
//        cmdLine.addArgument(cmd, false);
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        DefaultExecutor executor = new DefaultExecutor();
//        executor.setWorkingDirectory(new File(System.getenv("ZEPPELIN_NOTEBOOK_DIR")));
//        executor.setStreamHandler(new PumpStreamHandler(outputStream, System.err, System.in));
//        executor.setWatchdog(new ExecuteWatchdog(commandTOut));
//        try {
//            int exitValue = executor.execute(cmdLine);
//            return new InterpreterResult(InterpreterResult.Code.SUCCESS, outputStream.toString());
//        } catch (ExecuteException e) {
//            log.error("Can not run " + cmd, e);
//            return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
//        } catch (IOException e) {
//            log.error("Can not run " + cmd, e);
//            return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
//        }
//    }
//}
//
