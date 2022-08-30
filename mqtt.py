from paho.mqtt import client as mqtt_client
import random, time

# 设置 MQTT Broker 连接地址，端口以及 topic，同时我们调用 Python random.randint 函数随机生成 MQTT 客户端 id。
broker = 'broker.emqx.io'
port = 1883
topic = "/python/mqtt"
client_id = f'python-mqtt-{random.randint(0, 1000)}'


# 编写连接回调函数 on_connect，该函数将在客户端连接后被调用，在该函数中可以依据 rc 来判断客户端是否连接成功。通常同时我们将创建一个 MQTT 客户端，该客户端将连接到 broker.emqx.io。
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


# 首先定义一个 while 循环语句，在循环中我们将设置每秒调用 MQTT 客户端 publish 函数向 /python/mqtt 主题发送消息。
def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


# 编写消息回调函数 on_message，该函数将在客户端从 MQTT Broker 收到消息后被调用，在该函数中我们将打印出订阅的 topic 名称以及接收到的消息内容。
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topic)
    client.on_message = on_message
