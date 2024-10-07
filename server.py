from datetime import datetime
import json
import os
import random
import threading
import logging
from kafka import KafkaConsumer, KafkaProducer
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer, Consumer, KafkaError
from threading import Thread

# Flask setup
app = Flask(__name__)
data_file = 'products.json'
orders_file = 'orders.json'
CORS(app)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Safe JSON deserializer
def safe_json_deserializer(m):
    try:
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON message: {m}. Error: {e}")
        return None

# Kafka Consumer and Producer setup
consumer = KafkaConsumer(
    'orden-de-compra',
    bootstrap_servers='localhost:9092',
    group_id='backend-consumer-group',
    value_deserializer=safe_json_deserializer
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'  # Ajusta esto a la dirección de tu broker de Kafka
KAFKA_TOPIC = '/novedades'

# Configuración del productor
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
}
producer = Producer(producer_config)

# Configuración del consumidor
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'casa-central-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

# Almacén en memoria para las novedades
novedades = []

# Función para consumir mensajes de Kafka en segundo plano
def consume_kafka_messages():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error de consumo: {msg.error()}")
                break
        
        value = json.loads(msg.value().decode('utf-8'))
        novedades.append(value)
        # Limitar la lista a las últimas 100 novedades, por ejemplo
        if len(novedades) > 100:
            novedades.pop(0)

# Iniciar el consumo de mensajes en un hilo separado
kafka_thread = Thread(target=consume_kafka_messages)
kafka_thread.daemon = True
kafka_thread.start()

def load_data(file):
    if os.path.exists(file):
        with open(file, 'r') as f:
            return json.load(f)
    return []

def save_data(data, file):
    with open(file, 'w') as f:
        json.dump(data, f, indent=4)

def update_product_stock(code, new_stock):
    products = load_data()

    for product in products:
        if product.get('code') == code:
            product['stock'] = new_stock
            logging.info(f"Updated stock for product with code {code} to {new_stock}.")
            break
    else:
        logging.warning(f"Product with code {code} not found.")

    # Save the updated products back to the file
    save_data(products)

def get_product_by_code(code):
    products = load_data()
    for product in products:
        if isinstance(product, dict) and product.get('code') == code:
            return product
    return None

# Generate a 10-digit random code
def generate_code():
    return str(random.randint(1000000000, 9999999999))


def send_to_novedades_topic(product_info):
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(product_info).encode('utf-8'))
        producer.flush()
        print(f"Mensaje enviado al topic {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka: {str(e)}")

@app.route('/products', methods=['POST'])
def create_product():
    products = load_data(data_file)
    new_product = request.json
    new_product['id'] = (products[-1]['id'] + 1) if products else 1
    new_product['code'] = generate_code()
    
    required_fields = ['name', 'sizes', 'photos', 'stock']
    if not all(field in new_product for field in required_fields):
        return jsonify({'error': 'Missing required fields'}), 400
    
    if not isinstance(new_product['sizes'], dict):
        return jsonify({'error': 'Sizes should be a dictionary'}), 400
    
    for size, colors in new_product['sizes'].items():
        if not isinstance(colors, list):
            return jsonify({'error': f'Colors for size {size} should be a list'}), 400
    
    if not isinstance(new_product['photos'], list):
        return jsonify({'error': 'Photos should be a list of URLs'}), 400
    
    if not isinstance(new_product['stock'], int) or new_product['stock'] < 0:
        return jsonify({'error': 'Stock should be a non-negative integer'}), 400
    
    products.append(new_product)
    save_data(products, data_file)
    
    novedades_info = {
        'code': new_product['code'],
        'sizes': new_product['sizes'],
        'photos': new_product['photos'],
        'stock': new_product['stock']
    }
    
    send_to_novedades_topic(novedades_info)
    
    return jsonify(new_product), 201

@app.route('/novedades', methods=['GET'])
def get_novedades():
    return jsonify(novedades)

@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({'message': 'Missing JSON in request'}), 400
    
    data = request.get_json()
    
    if not data:
        return jsonify({'message': 'Invalid JSON'}), 400
    
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'message': 'Missing username or password'}), 400
    
    if username == 'casa_central' and password == 'password':
        return jsonify({'message': 'Login successful', 'role': 'casa_central'}), 200
    else:
        return jsonify({'message': 'Invalid credentials'}), 401

def inicializador_de_ordenes():
    if not os.path.exists(orders_file):
        sample_orders = [
            {
                "id": 1,
                "status": "paused",
                "items": [
                    {"product_id": 1, "quantity": 2},
                    {"product_id": 2, "quantity": 1}
                ]
            },
            {
                "id": 2,
                "status": "processing",
                "items": [
                    {"product_id": 3, "quantity": 1},
                    {"product_id": 4, "quantity": 3}
                ]
            },
            {
                "id": 3,
                "status": "paused",
                "items": [
                    {"product_id": 2, "quantity": 5},
                    {"product_id": 5, "quantity": 2}
                ]
            }
        ]
        save_data(sample_orders, orders_file)
        print("Initialized orders.json with sample data")
    else:
        print("orders.json already exists")


# Start processing messages from the consumer
def start_kafka_consumer():
    for message in consumer:
        orden_compra = message.value
        if orden_compra is None:
            logging.warning("Received an invalid message. Skipping...")
            continue

        logging.info(f"Received Order: {orden_compra}")
        
        codigo_tienda = orden_compra.get('codigo_tienda')
        if not codigo_tienda:
            logging.error("Order does not contain 'codigo_tienda'. Skipping...")
            continue

        topic_solicitudes = f"{codigo_tienda}_solicitudes"
        topic_despacho = f"{codigo_tienda}_despacho"
        items = orden_compra.get('items', [])
        fecha_solicitud = datetime.now().isoformat()

        # Load products before processing orders
        products = load_data()

        for item in items:
            producto_code = item.get('codigo_producto')
            cantidad = item.get('cantidad')

            if not producto_code or not isinstance(cantidad, int):
                logging.error(f"Invalid item data: {item}. Skipping item...")
                continue

            product = get_product_by_code(producto_code)

            if product:
                if product['stock'] >= cantidad > 0:
                    # Update stock for accepted product
                    product['stock'] -= cantidad
                    
                    # Create a shipping order
                    despacho_id = random.randint(1000, 9999)  # Generate a random dispatch ID
                    fecha_estimacion_envio = datetime.now().isoformat()  # You can replace with your logic
                    
                    # Construct a response for the accepted product
                    response = {
                        'estado': 'ACEPTADA',
                        'observaciones': f'Producto: {producto_code}. Orden aceptada.',
                        'fecha_solicitud': fecha_solicitud
                    }

                    # Send the response for the accepted product
                    producer.send(topic_solicitudes, value=response)
                    logging.info(f"Sent Response to {topic_solicitudes}: {response}")

                    # Send shipping order for accepted product
                    despacho = {
                        'id_despacho': despacho_id,
                        'id_orden_compra': orden_compra.get('id_orden_compra'),
                        'fecha_estimacion_envio': fecha_estimacion_envio
                    }
                    producer.send(topic_despacho, value=despacho)
                    update_product_stock(product['code'], product['stock'])
                    logging.info(f"Sent Dispatch Order to {topic_despacho}: {despacho}")
                else:
                    # Construct a response for rejected product
                    if product['stock'] < cantidad:
                        response = {
                            'estado': 'ACEPTADA',
                            'observaciones': f'Producto: {producto_code} no tiene suficiente stock. Solicitud queda pendiente.',
                            'fecha_solicitud': fecha_solicitud
                        }
                    else:
                        response = {
                            'estado': 'RECHAZADA',
                            'observaciones': f'Producto: {producto_code}. La cantidad para es erronea. Verfique la solicitud.',
                            'fecha_solicitud': fecha_solicitud
                        }
                    
                    # Send the response for the rejected product
                    producer.send(topic_solicitudes, value=response)
                    logging.info(f"Sent Response to {topic_solicitudes}: {response}")
            else:
                # Construct a response for a non-existent product
                response = {
                    'estado': 'RECHAZADA',
                    'observaciones': f'Producto: {producto_code} no existe.',
                    'fecha_solicitud': fecha_solicitud
                }
                # Send the response for the non-existent product
                producer.send(topic_solicitudes, value=response)
                logging.info(f"Sent Response to {topic_solicitudes}: {response}")
# Start Kafka Consumer in a separate thread
kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
kafka_thread.start()

if __name__ == '__main__':
    app.run(debug=True, port=5000)