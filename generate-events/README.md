# Generating events from time series data

Sometimes a use case requires generating events from time series values. These events can then be use to trigger additional processing, or can be used to define a time wondow for processing.

The code here provides a simple example. There are three files:

`gen-press.py` - simulates pressure values of a process. You run this on the command line. You'll need to install Quix Streams and obtain an SDK token from within Quix. This is covered in the Quix documentation. This is also covered in the Quickstart and the Quix Tour.

`generate-events.py` - this is the main service code to generate events from time series values. It uses the events to delimit a time window in which average pressure is calculated. Both events are published to an output topic, and the average pressure within the time window is also published to an output topic.

`handle-events.py` - shows a simple event handler routine that could be used to handle the genrated events in another process in the pipeline.

See also the [Quix documentation](https://quix.io/docs).
