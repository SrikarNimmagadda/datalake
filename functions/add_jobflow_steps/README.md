# add_jobflow_steps

This is the most extensible of the three lambda codebases in this project.
Lambdas generated from this codebase are intended to create a list of
EMR jobflow steps and have them added to the cluster on a regular schedule.

To create a new lambda based on this codebase, you basically only have to
add a new `step_builder` class and add the function definition
to the `serverless.yml` file.

However, before getting into the details of how to extend it, let's take a
look at the different files to see how this thing works.

## Files

### handler.py

This is the entry point to the lambda. This file acts as the composition
root--it's where we create whatever objects we're going to need, and then
set them in motion. It's also where we choose which set of steps we are
going to build. More on that later.

### cluster_finder.py

The class ClusterFinder in this module takes the cloudformation stack
name of the EMR cluster (which comes in on an environment variable) and
asks it what the cluster_id is. *Why don't we just pass the cluster_id
on the environment variable?* Because if the cluster is destroyed and
recreated, it will get a new cluster_id, but it will still have been
created with the same stack name.

### cf.py

This is just a little utility test program I wrote to get the output
of the cloudformation describe_stack method. It also acts as a live test
of the ClusterFinder class. Not used in production.

### step_factory.py

The StepFactory class inside this module is the code that creates an
individual step given its name, script and script arguments. Everything
else is defaulted.

### step_builder_xxx.py

The StepBuilder classes are the classes that describe the list of steps
to be created. You'll create a new one of these for every different lambda
you want to create to build steps on different schedules.

### tests

Your unit tests go here. We need a lot more.

## Creating New Lambdas

To add a new lambda that runs on a new schedule and creates different steps,
you don't need to create a new folder and copy/paste all of these files into
it. You will only need to add two files and change two files.

### Additions

You must create a new step_builder module with your new step_builder class
inside. It's easy to copy an existing one and change the names to protect
the innocent. This is where you'll do most of your work.

To go along with your new step_builder module, you'll want a new unit test
module to make sure it works.

### Modifications

Now, change the serverless.yml file to add your function definition.
Just copy one of the existing add_jobflow_step function defintions and change
the following properties in your copy:

* the top level resource identifier
* name
* events.schedule.name
* events.schedule.description
* events.schedule.rate
* events.schedule.input.builder (give it a unique tag to identify it)

(note that you don't change the package.artifact--you'll use the same one for
all add_jobflow_steps lambdas)

Finally, change handler.py's choose_builder method to add your lambda in
an `elif` clause, using the new value from events.schedule.input.builder.
