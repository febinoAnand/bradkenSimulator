import paho.mqtt.client as mqtt
import json
import time
import threading
from datetime import datetime

MQTT_BROKER = "134.209.149.69"
MQTT_PORT = 1883
MQTT_USERNAME = "admin"
MQTT_PASSWORD = "admin"

device_token = "Test1"

base_payload_template = {
    "device_token": device_token,
    "kW_Tot": 0.0000,
    "kW_R": 0.0000,
    "kW_Y": 0.0000,
    "kW_B": 0.0000,
    "Var_Tot": 0.0000,
    "PF_Avg": 0.0000,
    "PF_R": 0.0000,
    "PF_Y": 0.0000,
    "PF_B": 0.0000,
    "VA_Tot": 0.0000,
    "VA_R": 0.0000,
    "VA_Y": 0.0000,
    "VA_B": 0.0000,
    "VLL_Avg": 0.0000,
    "V_RY": 0.0000,
    "V_YB": 0.0000,
    "V_BR": 0.0000,
    "VLN_Avg": 0.0000,
    "V_R": 0.0000,
    "V_Y": 0.0000,
    "V_B": 0.0000,
    "Cu_Aug": 0.0000,
    "Cu_R": 0.0000,
    "Cu_Y": 0.0000,
    "Cu_B": 0.0000,
    "Fre_Hz": 0.0000,
    "Wh": 0.0000,
    "Vah": 0.0000,
    "Ind_VARh": 0.0000,
    "Cap_VARh": 0.0000,
    "VHar_R": 0.0000,
    "VHar_Y": 0.0000,
    "VHar_B": 0.0000,
    "CuHar_R": 0.0000,
    "CuHar_Y": 0.0000,
    "CuHar_B": 0.0000,
    "kWh_R": 0.0000,
    "kWh_Y": 0.0000,
    "kWh_B": 0.0000,
    "kVAh_R": 0.0000,
    "kVAh_Y": 0.0000,
    "kVAh_B": 0.0000,
    "PF_Avg_R": 0.0000,
    "PF_Avg_Y": 0.0000,
    "PF_Avg_B": 0.0000,
    "Cu_Avg_R": 0.0000,
    "Cu_Avg_Y": 0.0000,
    "Cu_Avg_B": 0.0000,
    "timestamp": int(time.time()),
}

incremental_step = {key: 0.1 for key in base_payload_template if isinstance(base_payload_template[key], float)}

all_machines = [f"test{i}" for i in range(1, 31)]

def update_timestamp(payload, machine_index, base_timestamp):
    timestamp = base_timestamp + machine_index
    payload["timestamp"] = timestamp
    return payload

def on_message(client, userdata, msg):
    print(f"Received message from topic {msg.topic}: {msg.payload.decode()}")

def simulate_device_for_token(device_token, assigned_machines):
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe("default_subtopic/#")
    client.on_message = on_message
    client.loop_start()

    machine_states = {machine: base_payload_template.copy() for machine in assigned_machines}

    while True:
        base_timestamp = int(time.time())
        
        for idx, machine in enumerate(assigned_machines):
            current_payload = machine_states[machine]

            for key in incremental_step.keys():
                current_payload[key] += incremental_step[key]

            current_payload = update_timestamp(current_payload, idx, base_timestamp)

            json_payload = json.dumps(current_payload)

            topic = f"default_subtopic/{device_token}/{machine}"

            client.publish(topic, json_payload)

            print(f"Device {device_token}, Machine {machine} sent data to topic {topic}: {json_payload}")

        time.sleep(60)

def simulate_devices():
    threads = []
    for token in [device_token]:
        thread = threading.Thread(target=simulate_device_for_token, args=(token, all_machines))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    simulate_devices()