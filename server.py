from datetime import datetime
import json
import os
import random
import threading
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer, Consumer, KafkaError
from threading import Thread

app = Flask(__name__)
data_file = 'products.json'
orders_file = 'orders.json'
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_NOVEDADES = 'novedades'
KAFKA_TOPIC_ORDEN_COMPRA = 'orden-de-compra'

# Configuración del productor
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
}
producer = Producer(producer_config)

# Configuración del consumidor
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'backend-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC_ORDEN_COMPRA])

# Almacén en memoria para las novedades
novedades = []

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

kafka_thread = Thread(target=consume_kafka_messages)
kafka_thread.start()

def send_to_novedades_topic(product_info):
    try:
        producer.produce(KAFKA_TOPIC_NOVEDADES, json.dumps(product_info).encode('utf-8'))
        producer.flush()
        print(f"Mensaje enviado al topic {KAFKA_TOPIC_NOVEDADES}")
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka: {str(e)}")

def inicializador_de_ordenes():
    if not os.path.exists(orders_file):
        sample_orders = [
            {
                "id": 1,
                "status": "pausado",
                "items": [
                    {"product_id": 1, "quantity": 2},
                    {"product_id": 2, "quantity": 1}
                ]
            },
            {
                "id": 2,
                "status": "procesando",
                "items": [
                    {"product_id": 3, "quantity": 1},
                    {"product_id": 4, "quantity": 3}
                ]
            },
            {
                "id": 3,
                "status": "pausado",
                "items": [
                    {"product_id": 2, "quantity": 5},
                    {"product_id": 5, "quantity": 2}
                ]
            }
        ]
        save_data(sample_orders, orders_file)
        print("orders.json se inicializó con información de ejemplo correctamente")
    else:
        print("orders.json ya existe")

def load_data(file):
    if os.path.exists(file):
        with open(file, 'r') as f:
            return json.load(f)
    return []

def save_data(data, file):
    with open(file, 'w') as f:
        json.dump(data, f, indent=4)

def update_product_stock(code, new_stock):
    products = load_data(data_file)
    for product in products:
        if product.get('code') == code:
            product['stock'] = new_stock
            logging.info(f"Updated stock for product with code {code} to {new_stock}.")
            break
    else:
        logging.warning(f"Product with code {code} not found.")
    save_data(products, data_file)

def get_product_by_code(code):
    products = load_data(data_file)
    for product in products:
        if isinstance(product, dict) and product.get('code') == code:
            return product
    return None

def generate_code():
    return str(random.randint(1000000000, 9999999999))

def send_to_kafka(topic, message):
    try:
        producer.produce(topic, json.dumps(message).encode('utf-8'))
        producer.flush()
        logging.info(f"Mensaje enviado al tema {topic}")
    except Exception as e:
        logging.error(f"Error al enviar mensaje a Kafka: {str(e)}")

@app.route('/products', methods=['POST'])
def create_product():
    products = load_data(data_file)
    new_product = request.json
    new_product['id'] = (products[-1]['id'] + 1) if products else 1
    new_product['code'] = generate_code()
    
    required_fields = ['name', 'sizes', 'photos', 'stock']
    if not all(field in new_product for field in required_fields):
        return jsonify({'error': 'Faltan campos requeridos'}), 400
    
    if not isinstance(new_product['sizes'], dict):
        return jsonify({'error': 'Los tamaños deben ser un diccionario'}), 400
    
    for size, colors in new_product['sizes'].items():
        if not isinstance(colors, list):
            return jsonify({'error': f'Los colores para el tamaño {size} deben ser una lista'}), 400
    
    if not isinstance(new_product['photos'], list):
        return jsonify({'error': 'Las fotos deben ser una lista de URLs'}), 400
    
    if not isinstance(new_product['stock'], int) or new_product['stock'] < 0:
        return jsonify({'error': 'El stock debe ser un entero no negativo'}), 400
    
    products.append(new_product)
    save_data(products, data_file)
    
    novedades_info = {
        'code': new_product['code'],
        'sizes': new_product['sizes'],
        'photos': new_product['photos']
    }
    
    send_to_kafka(KAFKA_TOPIC_NOVEDADES, novedades_info)
    
    return jsonify(new_product), 201

@app.route('/novedades', methods=['GET'])
def get_novedades():
    return jsonify(novedades)

@app.route('/orders', methods=['POST'])
def create_order():
    orders = load_data(orders_file)
    products = load_data(data_file)
    new_order = request.json
    new_order['id'] = (orders[-1]['id'] + 1) if orders else 1
    
    can_fulfill = True
    for item in new_order['items']:
        product = next((p for p in products if p['id'] == item['product_id']), None)
        if not product or product['stock'] < item['quantity']:
            can_fulfill = False
            break
    
    if can_fulfill:
        new_order['status'] = 'processing'
        for item in new_order['items']:
            product = next((p for p in products if p['id'] == item['product_id']), None)
            product['stock'] -= item['quantity']
    else:
        new_order['status'] = 'paused'
    
    orders.append(new_order)
    save_data(orders, orders_file)
    save_data(products, data_file)
    return jsonify(new_order), 201

@app.route('/products/<int:id>/stock', methods=['PUT'])
def update_stock(id):
    products = load_data(data_file)
    product = next((p for p in products if p['id'] == id), None)
    if product:
        new_stock = request.json.get('stock')
        if new_stock is not None:
            product['stock'] = new_stock
            save_data(products, data_file)
            reprocess_paused_orders()
            return jsonify(product)
        return jsonify({'error': 'Stock value not provided'}), 400
    return jsonify({'error': 'Product not found'}), 404

def reprocess_paused_orders():
    orders = load_data(orders_file)
    products = load_data(data_file)
    
    for order in orders:
        if order['status'] == 'paused':
            can_fulfill = True
            for item in order['items']:
                product = next((p for p in products if p['id'] == item['product_id']), None)
                if not product or product['stock'] < item['quantity']:
                    can_fulfill = False
                    break
            
            if can_fulfill:
                for item in order['items']:
                    product = next((p for p in products if p['id'] == item['product_id']), None)
                    product['stock'] -= item['quantity']
                order['status'] = 'processing'
    
    save_data(orders, orders_file)
    save_data(products, data_file)

@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({'message': 'Falta JSON en la solicitud'}), 400
    
    data = request.get_json()
    
    if not data:
        return jsonify({'message': 'JSON inválido'}), 400
    
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'message': 'Falta nombre de usuario o contraseña'}), 400
    
    if username == 'casa_central' and password == 'password':
        return jsonify({'message': 'Inicio de sesión exitoso', 'role': 'casa_central'}), 200
    else:
        return jsonify({'message': 'Credenciales inválidas'}), 401

def start_kafka_consumer():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Error de consumo: {msg.error()}")
                break
        
        try:
            orden_compra = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError:
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

        products = load_data(data_file)

        for item in items:
            producto_code = item.get('codigo_producto')
            cantidad = item.get('cantidad')

            if not producto_code or not isinstance(cantidad, int):
                logging.error(f"Invalid item data: {item}. Skipping item...")
                continue

            product = get_product_by_code(producto_code)

            if product:
                if product['stock'] >= cantidad > 0:
                    product['stock'] -= cantidad
                    
                    despacho_id = random.randint(1000, 9999)
                    fecha_estimacion_envio = datetime.now().isoformat()
                    
                    response = {
                        'estado': 'ACEPTADA',
                        'observaciones': f'Producto: {producto_code}. Orden aceptada.',
                        'fecha_solicitud': fecha_solicitud
                    }

                    send_to_kafka(topic_solicitudes, response)

                    despacho = {
                        'id_despacho': despacho_id,
                        'id_orden_compra': orden_compra.get('id_orden_compra'),
                        'fecha_estimacion_envio': fecha_estimacion_envio
                    }
                    send_to_kafka(topic_despacho, despacho)
                    update_product_stock(product['code'], product['stock'])
                else:
                    if product['stock'] < cantidad:
                        response = {
                            'estado': 'ACEPTADA',
                            'observaciones': f'Producto: {producto_code} no tiene suficiente stock. Solicitud queda pendiente.',
                            'fecha_solicitud': fecha_solicitud
                        }
                    else:
                        response = {
                            'estado': 'RECHAZADA',
                            'observaciones': f'Producto: {producto_code}. La cantidad es errónea. Verifique la solicitud.',
                            'fecha_solicitud': fecha_solicitud
                        }
                    
                    send_to_kafka(topic_solicitudes, response)
            else:
                response = {
                    'estado': 'RECHAZADA',
                    'observaciones': f'Producto: {producto_code} no existe.',
                    'fecha_solicitud': fecha_solicitud
                }
                send_to_kafka(topic_solicitudes, response)

# Iniciar el consumo de mensajes en un hilo separado
kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
kafka_thread.start()

if __name__ == '__main__':
    inicializador_de_ordenes()
    app.run(debug=True, port=5000)