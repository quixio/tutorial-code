# Source Code: Build a simple event-driven system to get ML predictions with Python and Apache Kafka

The folder stores the code that accompanies the tutorial "[Build a simple event-driven system to get ML predictions with Python and Apache Kafka](#)" first published on the Quix Blog.

The tutorial shows you how to create a system on your local machine that emulates an event-based architecture. In this architecture, an email server is interacting with a machine learning server to get spam predictions. Kafka is in the middle managing the data flow.
* The email server sends a stream of emails to the ML server via Kafka
* For each email, the ML server returns a prediction about whether the email is spam or “ham” (a normal email)
* It sends the predictions back to the email server, also via Kafka
* The mail server can then update the spam label in its email database

**NOTE**: Originally, the tutorial used the `kafka-python` library but has now been updated to use the [Quix Streams](https://github.com/quixio/quix-streams) library. You can still find the old versions of the code in the subfolder [kafka-python-version](kafka-python-version).
