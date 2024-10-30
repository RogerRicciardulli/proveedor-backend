from datetime import datetime
import json
import os
import random
import threading
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer, Consumer, KafkaError

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(BASE_DIR, 'products.json')
orders_file = os.path.join(BASE_DIR, 'orders.json')
CORS(app)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC_NOVEDADES = 'novedades'
KAFKA_TOPIC_ORDEN_COMPRA = 'orden-de-compra'

producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
}
producer = Producer(producer_config)
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'mi-grupo',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'fetch.max.bytes': 1048576,
    'max.partition.fetch.bytes': 1048576,
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC_ORDEN_COMPRA])

novedades = []

def load_data(file):
    """Cargar datos desde un archivo JSON"""
    try:
        if os.path.exists(file) and os.path.getsize(file) > 0:
            with open(file, 'r') as f:
                return json.load(f)
        else:
            logging.info(
                f"Archivo {file} no existe o está vacío. Retornando lista vacía.")
            return []
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar JSON desde {file}: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Error al cargar datos desde {file}: {str(e)}")
        return []

def save_data(data, file):
    """Guardar datos en un archivo JSON"""
    try:
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Datos guardados exitosamente en {file}")
    except Exception as e:
        logging.error(f"Error al guardar datos en {file}: {str(e)}")

def update_product_stock(code, new_stock):
    products = load_data(data_file)
    for product in products:
        if product.get('code') == code:
            product['stock'] = new_stock
            logging.info(
                f"Updated stock for product with code {code} to {new_stock}.")
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

def save_order(order_data):
    orders = load_data(orders_file)
    order_data["fecha_solicitud"] = datetime.now().isoformat()
    order_data["fecha_recepcion"] = None
    orders.append(order_data)
    save_data(orders, orders_file)
    logging.info(f"Orden de compra guardada: {order_data}")

def save_order_to_db(item, estado, observaciones, despacho_id=None, fecha_solicitud=None):
    orders_db = load_data(orders_file)
    order_id = (orders_db[-1]["id"] + 1) if orders_db else 1
    order_data = {
        "id": order_id,
        "items": item,
        "estado": estado,
        "observaciones": observaciones,
        "orden_despacho": despacho_id,
        "fecha_solicitud": fecha_solicitud,
        "fecha_recepcion": None 
    }
    orders_db.append(order_data)
    save_data(orders_db, orders_file)
    logging.info("Order saved to database.")

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
        try:
            value = json.loads(msg.value().decode('utf-8'))
            novedades.append(value)
            consumer.commit(msg)
            print(f"Mensaje confirmado: {value}")
        except json.JSONDecodeError:
            logging.warning("Error al decodificar JSON. Saltando mensaje...")
        except Exception as e:
            logging.error(f"Error procesando el mensaje: {str(e)}")

def send_to_kafka(topic, message):
    try:
        producer.produce(topic, json.dumps(message).encode('utf-8'))
        producer.flush()
        logging.info(f"Mensaje enviado al tema {topic}. Message: {message}")
    except Exception as e:
        logging.error(f"Error al enviar mensaje a Kafka: {str(e)}")

def reprocess_pending_orders(product_code):
    orders_db = load_data(orders_file)
    pending_observation = f"Producto: {product_code} no tiene suficiente stock. Solicitud queda pendiente."
    reprocessed_count = 0

    for order in orders_db:
        if order['estado'] == 'ACEPTADA' and order['observaciones'] == pending_observation:
            can_fulfill = True
            
            items = order.get('items', [])
            for item in items:
                if item['codigo'] == product_code:
                    product = get_product_by_code(product_code)
                    if product and product['stock'] >= item['cantidad']:
                        product['stock'] -= item['cantidad']
                        order['estado'] = 'ACEPTADA'
                        order['observaciones'] = f"Producto: {product_code}. Orden aceptada."
                        despacho_id = random.randint(1000, 9999)
                        fecha_estimacion_envio = datetime.now().isoformat()
                        
                        # Create a dispatch record and update order
                        despacho = {
                            'idDespacho': despacho_id,
                            'idOrden': order['id'],
                            'fecha_estimacion_envio': fecha_estimacion_envio
                        }
                        send_to_kafka(f"{order['id']}-despacho", despacho)
                        update_product_stock(product_code, product['stock'])
                        save_data(orders_db, orders_file)
                        
                        logging.info(f"Order {order['id']} reprocessed and dispatched.")
                        reprocessed_count += 1
                    else:
                        can_fulfill = False
                        break
            if can_fulfill:
                # Update order with dispatch info
                order['orden_despacho'] = despacho_id
                order['fecha_recepcion'] = fecha_estimacion_envio
                
    save_data(orders_db, orders_file)
    logging.info(f"{reprocessed_count} pending orders reprocessed for product {product_code}.")

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
            consumer.commit(msg)
            orden_compra = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError:
            logging.warning("Received an invalid message. Skipping...")
            continue
        logging.info(f"Received Order: {orden_compra}")
        codigo_tienda = orden_compra.get('id_tienda')
        if not codigo_tienda:
            logging.error(
                "Order does not contain 'codigo_tienda'. Skipping...")
            continue
        topic_solicitudes = f"{codigo_tienda}-solicitudes"
        topic_despacho = f"{codigo_tienda}-despacho"
        items = orden_compra.get('orders', [])
        fecha_solicitud = datetime.now().isoformat()
        for item in items:
            producto_code = item.get('codigo')
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
                    print(response)
                    send_to_kafka(topic_solicitudes, response)
                    despacho = {
                        'idDespacho': despacho_id,
                        'idOrden': orden_compra.get('idOrden'),
                        'fecha_estimacion_envio': fecha_estimacion_envio
                    }
                    send_to_kafka(topic_despacho, despacho)
                    update_product_stock(product['code'], product['stock'])
                    save_order_to_db(item, response['estado'], response['observaciones'], despacho_id, fecha_solicitud)
                else:
                    if product['stock'] < cantidad:
                        response = {
                            'estado': 'ACEPTADA',
                            'observaciones': f'Producto: {producto_code} no tiene suficiente stock. Solicitud queda pendiente.',
                            'fecha_solicitud': fecha_solicitud
                        }
                        save_order_to_db(item, response['estado'], response['observaciones'], None, fecha_solicitud)
                    else:
                        response = {
                            'estado': 'RECHAZADA',
                            'observaciones': f'Producto: {producto_code}. La cantidad es errónea. Verifique la solicitud.',
                            'fecha_solicitud': fecha_solicitud
                        }
                        save_order_to_db(item, response['estado'], response['observaciones'], None, fecha_solicitud)
                    send_to_kafka(topic_solicitudes, response)
            else:
                response = {
                    'estado': 'RECHAZADA',
                    'observaciones': f'Producto: {producto_code} no existe.',
                    'fecha_solicitud': fecha_solicitud
                }
                save_order_to_db(item, response['estado'], response['observaciones'], None, fecha_solicitud)
                send_to_kafka(topic_solicitudes, response)

## -------------------- Metodos REST para la comunicacion con el front de proveedor -------------------- ##
@app.route('/products', methods=['POST'])
def create_product():
    products = load_data(data_file)
    new_product = request.json
    new_product['id'] = (products[-1]['id'] + 1) if products else 1
    new_product['code'] = generate_code()
    products.append(new_product)
    save_data(products, data_file)
    return jsonify(new_product), 201

@app.route('/products', methods=['GET'])
def get_products():
    products = load_data(data_file)
    return jsonify(products)

@app.route('/products/<int:id>', methods=['GET'])
def get_product(id):
    products = load_data(data_file)
    product = next((p for p in products if p['id'] == id), None)
    if product:
        return jsonify(product)
    return jsonify({'error': 'Product not found'}), 404

@app.route('/products/<int:id>', methods=['PUT'])
def update_product(id):
    products = load_data(data_file)
    product = next((p for p in products if p['id'] == id), None)
    if product:
        updated_data = request.json
        product.update(updated_data)
        save_data(products, data_file)
        #reprocess_pending_orders(product['code'])
        return jsonify(product)
    return jsonify({'error': 'Product not found'}), 404
## -------------------- ------------------------------------------------------------ -------------------- ##

kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
kafka_thread.start()

if __name__ == '__main__':
    app.run(debug=True, port=5001)