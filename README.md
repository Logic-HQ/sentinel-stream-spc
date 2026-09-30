# Sentinel-stream Statistical Process Control
## A real-time stream processing and data pipeline for statistical process control (SPC) and continuous anomaly detection."



## ⚙️ Core Architecture Style
Real-Time SPC & Anomaly Detection in streaming pipelines based on strategic mix of core technologies, domain-specific categories:

> * 📊 real-time-analytics • anomaly-detection • statistical-process-control • real-time • stream-processing • data-pipeline • event-driven predictive-maintenance • 📈 quality-control • monitoring • observability • time-series • apache-kafka • apache-spark • apache-spark • python



## Domain Use Cases 

• Natural Fit for SPC: Statistical Process Control was literally invented for manufacturing and industrial processes. SPC relies on continuous, normally distributed variables (like temperature, vibration, pressure, or voltage). Calculating running averages, standard deviations, and control limits (like Western Electric Rules) makes perfect sense here.
• Predictable Schema: IoT sensors emit uniform, predictable data points at steady intervals. This makes it much easier to build your Moving Window Metrics (Task 1) and Z-Score Evaluation (Task 2) without constantly fighting missing data fields.
• Multivariate Anomalies: For your Isolation Forest (Task 3), an industrial machine often fails due to a combination of factors (e.g., high temperature and low RPM). This provides a compelling story for why you needed a machine learning model alongside traditional SPC limits.

![streaming-windows](https://github.com/Logic-HQ/sentinel-stream-spc/blob/main/docs/img/streaming-windows.gif)

While the math formulas themselves—like calculating a standard deviation (\(\sigma \)) or a Z-score \(frac{X - 𝜇 }/{𝜎}\))—are simple level math, when you move from a static Jupyter Notebook to PySpark Structured Streaming, implementing them in a distributed, real-time streaming pipeline is deceptively difficult and drives in into the challenges of distributed systems engineering. 


To solve complex streaming edge cases.

<details><summary><h3>1. The Streaming State Management Challenge (The Biggest Hurdle)</h3></summary>


In a notebook, calculating a moving average over the last 100 rows is one line of code. In PySpark Streaming, data arrives sequentially across a distributed cluster.
• The Problem: Spark needs to "remember" the previous sensor readings to calculate the current rolling average and standard deviation. This requires stateful streaming (mapGroupsWithState or windowed aggregations).
• The Danger: If your sensor emits data every 100ms and you keep a massive window in memory, your Spark executors will eventually run out of memory (OOM errors) and crash. Managing the memory footprint of this "state" while pruning old data is a significant engineering challenge.
</details>
<details><summary><h3>2. Time Semantic Confusions: Event Time vs. Processing Time</h3></summary>


IoT sensors exist in the physical world. Network drops, intermittent Wi-Fi, or edge device lag mean data will arrive late or out of order.
• Processing Time: The time the event hits your Kafka broker or PySpark engine.
• Event Time: The actual timestamp when the sensor recorded the metric on the machine.
• The Challenge: For accurate SPC control charts, you must use Event Time. If a sensor disconnects for 5 minutes and then dumps 3,000 delayed messages into Kafka at once, a naive pipeline using processing time will see a massive "spike" in throughput and flag false anomalies. You must implement Watermarking in PySpark to elegantly handle late-arriving data.
</details>
<details><summary><h3>3. "Bootstrapping" and Dynamic Control Limits</h3></summary>


Traditional SPC assumes you already know what "normal" looks like so you can draw your Upper and Lower Control Limits (UCL/LCL).
• The Catch-22: When your streaming pipeline first turns on, it has no history. It doesn't know the mean (\(\mu \)) or standard deviation (\(\sigma \)) yet.
• The Engineering Fix: You have to build a "warm-up" or bootstrapping phase. Your pipeline must safely collect an initial baseline batch of data to calculate the starting SPC limits before it starts aggressively flagging Z-score anomalies, or dynamically update the limits over time without letting past anomalies skew the calculation.
</details>
<details><summary><h3>3. "Bootstrapping" and Dynamic Control Limits</h3></summary>
4. Itegration of lineage Between Micro-Batches and ML

Your architecture utilizes an Isolation Forest for multivariate anomalies.
• The Architectural Split: PySpark Structured Streaming operates on rapid micro-batches (e.g., processing data every 1 or 2 seconds). However, training or running inference on a Machine Learning model like an Isolation Forest usually requires a wider window of data to look at feature correlations.
• The Challenge: Passing data smoothly from the low-latency SPC alerts (Task 1 & 2) into a micro-batched ML model inference loop (Task 3) without introducing massive bottleneck lag requires careful tuning of Spark's trigger intervals and memory caching.
</details>




