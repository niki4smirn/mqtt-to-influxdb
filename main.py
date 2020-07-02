import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# ip-адрес influxdb
INFLUXDB_ADDRESS = ''
# название базы данных в influxdb
INFLUXDB_DATABASE = ''

# ip-адрес mqtt сервера
MQTT_ADDRESS = ''
# топик, который будет слушать скрипт
MQTT_TOPIC = ''
# уникальный id для подключения к mqtt-сети
MQTT_CLIENT_ID = ''

# список датчиков, данные с которых мы заносим в базу данных
SENSORS = ['DHT11', 'MHZ19B']
# для каждого датчика названия измерений, которые нам нужны
FIELDS_FOR_SENSORS = [['Temperature', 'Humidity'], ['CarbonDioxide']]
# словарь, который переводит из формата CapitalizedWords в формат
# lower_case_with_underscores 
TYPE_NAME = {'Temperature': 'temperature', 'Humidity': 'humidity',
             'CarbonDioxide': 'carbon_dioxide'}


# функция, которая будет вызываться при подключении к mqtt-серверу
def on_connect(client, userdata, flags, rc):
    # подписываемся на топик
    client.subscribe(MQTT_TOPIC)


# функция, которая будет вызываться при появлении сообщения в топиках,
# на которые мы подписались
def on_message(client, userdata, msg):
    # выводим в консоль с какого топика пришло сообщение и текст сообщения
    print(msg.topic + ' ' + str(json.loads(msg.payload)))
    # парсим строку в json запрос
    sensors_json = json.loads(msg.payload)
    # перебираем сенсоры
    for i in range(len(SENSORS)):
        # перебираем показатели сенсора
        for j in range(len(FIELDS_FOR_SENSORS[i])):
            # делаем json, который положим в базу данных
            json_body = [
                {
                    'measurement': TYPE_NAME[FIELDS_FOR_SENSORS[i][j]],
                    'fields': {
                        'value': (sensors_json[SENSORS[i]]
                                  [FIELDS_FOR_SENSORS[i][j]] * 1.0)
                    }
                }
            ]
            # записываем json в базу данных
            influxdb_client.write_points(json_body)


# инициализация базы данных
def _init_influxdb_database():
    # получаем список всех баз данных в influxdb
    databases = influxdb_client.get_list_database()
    # проверяем, если количество баз данных с названием INFLUXDB_DATABASE 
    # равна нулю, то создаем такую базу данных
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) \
            == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    # и переключаемся на неё
    influxdb_client.switch_database(INFLUXDB_DATABASE)


# подключаемся к influxdb
influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086)
# инициализируем бд
_init_influxdb_database()

# инициализируем mqtt-клиента
mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
# указываем, какие функции будут вызываться при подключении и
mqtt_client.on_connect = on_connect
# появлении нового сообщения
mqtt_client.on_message = on_message

# подключаемся к mqtt-серверу
mqtt_client.connect(MQTT_ADDRESS, 1883)
mqtt_client.loop_forever()
