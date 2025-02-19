import paho.mqtt.client as mqtt
import json
import time
import threading
import random
from datetime import datetime, timedelta


MQTT_BROKER = "emsmqtt.univa.cloud"
MQTT_PORT = 1883
MQTT_USERNAME = "admin"
MQTT_PASSWORD = "admin"

device_token = "TEST"

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

class UniqueTimestampGenerator:
    def __init__(self):
        self.generated_timestamps = set()

    def random_increment_timestamp(self, timestamp: datetime, min_increment: int = 1, max_increment: int = 60) -> datetime:
    
        if min_increment < 0 or max_increment < min_increment:
            raise ValueError("Invalid increment range: min_increment should be >= 0 and <= max_increment.")
        
        # Generate a new unique timestamp

        while True:
            increment_seconds = random.randint(min_increment, max_increment)
            new_timestamp = timestamp + timedelta(seconds=increment_seconds)
            
            new_timestamp_int = int(new_timestamp.timestamp())
            if new_timestamp_int not in self.generated_timestamps:
                self.generated_timestamps.add(new_timestamp_int)
                return new_timestamp_int


def generate_random_energy_data(device_token):
    # Template with dynamic device token and timestamp
    timestamp_generator = UniqueTimestampGenerator()

    original_timestamp = datetime.now()
    new_timestamp = timestamp_generator.random_increment_timestamp(original_timestamp, 10, 300)

    base_payload_template = {
        "device_token": device_token,
        "kW_Tot": round(random.uniform(1000, 1000000), 4),
        "kW_R": round(random.uniform(1000, 1000000), 4),
        "kW_Y": round(random.uniform(1000, 1000000), 4),
        "kW_B": round(random.uniform(1000, 1000000), 4),
        "Var_Tot": round(random.uniform(1000, 5000000), 4),
        "PF_Avg": round(random.uniform(-0.50, -0.99), 4),
        "PF_R": round(random.uniform(0.5, 1), 4),
        "PF_Y": round(random.uniform(0.5, 1), 4),
        "PF_B": round(random.uniform(0.5, 1), 4),
        "VA_Tot": round(random.uniform(0, 150000), 4),
        "VA_R": round(random.uniform(0, 150000), 4),
        "VA_Y": round(random.uniform(0, 150000), 4),
        "VA_B": round(random.uniform(0, 150000), 4),
        "VLL_Avg": round(random.uniform(220, 240), 4),
        "V_RY": round(random.uniform(220, 240), 4),
        "V_YB": round(random.uniform(220, 240), 4),
        "V_BR": round(random.uniform(220, 240), 4),
        "VLN_Avg": round(random.uniform(220, 240), 4),
        "V_R": round(random.uniform(220, 240), 4),
        "V_Y": round(random.uniform(220, 240), 4),
        "V_B": round(random.uniform(220, 240), 4),
        "Cu_Aug": round(random.uniform(0, 50), 4),
        "Cu_R": round(random.uniform(0, 50), 4),
        "Cu_Y": round(random.uniform(0, 50), 4),
        "Cu_B": round(random.uniform(0, 50), 4),
        "Fre_Hz": round(random.uniform(49.5, 50.5), 4),
        "Wh":  (new_timestamp * 4/1000),
        "Vah": (new_timestamp * 6/1000),
        "Ind_VARh": round(random.uniform(0, 50000), 4),
        "Cap_VARh": round(random.uniform(0, 50000), 4),
        "VHar_R": round(random.uniform(0, 20), 4),
        "VHar_Y": round(random.uniform(0, 20), 4),
        "VHar_B": round(random.uniform(0, 20), 4),
        "CuHar_R": round(random.uniform(0, 20), 4),
        "CuHar_Y": round(random.uniform(0, 20), 4),
        "CuHar_B": round(random.uniform(0, 20), 4),
        "kWh_R": round(random.uniform(0, 100000), 4),
        "kWh_Y": round(random.uniform(0, 100000), 4),
        "kWh_B": round(random.uniform(0, 100000), 4),
        "kVAh_R": round(random.uniform(0, 100000), 4),
        "kVAh_Y": round(random.uniform(0, 100000), 4),
        "kVAh_B": round(random.uniform(0, 100000), 4),
        "PF_Avg_R": round(random.uniform(0.5, 1), 4),
        "PF_Avg_Y": round(random.uniform(0.5, 1), 4),
        "PF_Avg_B": round(random.uniform(0.5, 1), 4),
        "Cu_Avg_R": round(random.uniform(0, 50), 4),
        "Cu_Avg_Y": round(random.uniform(0, 50), 4),
        "Cu_Avg_B": round(random.uniform(0, 50), 4),
        "timestamp": int(time.time()),
    }
    return base_payload_template


# incremental_step = {key: 0.1 for key in base_payload_template if isinstance(base_payload_template[key], float)}


all_machines = [f"TEST_1_{i}" for i in range(1, 31)]

def update_timestamp(payload, machine_index, base_timestamp):
    timestamp = base_timestamp + machine_index
    payload["timestamp"] = timestamp
    return payload

def on_message(client, userdata, msg):
    # print(f"Received message from topic {msg.topic}: {msg.payload.decode()}")
    pass

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
            current_payload = {}
            # current_payload = machine_states[machine]
            incremental_step = generate_random_energy_data(device_token)
            for key in incremental_step.keys():
                current_payload[key] = incremental_step[key]

            current_payload = update_timestamp(current_payload, idx, base_timestamp)

            json_payload = json.dumps(current_payload)

            topic = f"default_subtopic/{device_token}/{machine}"

            client.publish(topic, json_payload)

            print(f"Device {device_token}, Machine {machine} sent data to topic {topic}: {json_payload}")

        time.sleep(180)

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