
### Local Installation

To install Spark Notebook into your local workstation run:
```sh
 $ ROOT_FOLDER/sbt/sbt "clean reload compile stage"
```
#### Warning:
 - if you download the project and execute the sbt command dist it will create a file named sparknotebook-VERSION.zip into the target/universal/ folder, and this file will be deployed to your cluster. So any changes that you've done to the project will be deployed. To avoid this call sbt clean.

It will will compile and create an executable version of the SparkNotebook at:
```sh
  ROOT_FOLDER/target/universal/stage/bin
```

Before executing it, you will need to change some ipython configs. The first step is to create a new profile for Spark Notebook with following command:
```sh
 $ ipython profile create sparknotebook # create profile sparknotebook w/ default config files
```

Edit the profile file created at your home folder:
```sh
  $HOME/.ipython/profile_sparknotebook/profile_config.py
```
Replacing its content with the below:
```sh
    c = get_config()
    c.KernelManager.kernel_cmd = ["/PROJECT_ROOT_FOLDER/sparknotebook/target/universal/stage/bin/sparknotebook", 
    , "--profile", "{connection_file}",
     "--parent"]
    c.NotebookApp.ip = '*' # only add this line if you want IPython-notebook being open to the public
    c.NotebookApp.open_browser = False # only add this line if you want to suppress opening a browser after IPython-notebook initialization
```
Don't forget to substitute the **PROJECT_ROOT_FOLDER** content.
With this config, you can start running the spark notebook locally with the following command:

```sh
 $ ipython notebook --profile sparknotebook
```
Go to http://localhost:8888 to see it running

