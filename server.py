from datetime import datetime
import json
import os
import random
import threading
import logging
import csv
import io
import pymysql
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Producer, Consumer, KafkaError
from threading import Thread
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(BASE_DIR, 'products.json')
orders_file = os.path.join(BASE_DIR, 'orders.json')
CORS(app)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

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
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'fetch.max.bytes': 1048576,
    'max.partition.fetch.bytes': 1048576,
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
        producer.produce(KAFKA_TOPIC_NOVEDADES, json.dumps(
            product_info).encode('utf-8'))
        producer.flush()
        print(f"Mensaje enviado al topic {KAFKA_TOPIC_NOVEDADES}")
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka: {str(e)}")


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
    try:
        orders = load_data(orders_file)
        products = load_data(data_file)

        if not isinstance(orders, list):
            orders = []

        new_order = request.json
        if not new_order:
            return jsonify({'error': 'No se proporcionaron datos de la orden'}), 400

        new_order['id'] = (orders[-1]['id'] + 1) if orders else 1

        if 'items' not in new_order:
            return jsonify({'error': 'La orden debe contener items'}), 400

        can_fulfill = True
        for item in new_order['items']:
            if not isinstance(item, dict) or 'product_id' not in item or 'quantity' not in item:
                return jsonify({'error': 'Formato de item inválido'}), 400

            product = next(
                (p for p in products if p['id'] == item['product_id']), None)
            if not product or product['stock'] < item['quantity']:
                can_fulfill = False
                break

        if can_fulfill:
            new_order['status'] = 'processing'
            for item in new_order['items']:
                product = next(
                    (p for p in products if p['id'] == item['product_id']), None)
                product['stock'] -= item['quantity']
        else:
            new_order['status'] = 'paused'

        orders.append(new_order)
        save_data(orders, orders_file)
        save_data(products, data_file)
        return jsonify(new_order), 201

    except Exception as e:
        logging.error(f"Error al crear orden: {str(e)}")
        return jsonify({'error': 'Error interno al procesar la orden'}), 500


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
                product = next(
                    (p for p in products if p['id'] == item['product_id']), None)
                if not product or product['stock'] < item['quantity']:
                    can_fulfill = False
                    break

            if can_fulfill:
                for item in order['items']:
                    product = next(
                        (p for p in products if p['id'] == item['product_id']), None)
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

        codigo_tienda = orden_compra.get('idTienda')
        if not codigo_tienda:
            logging.error(
                "Order does not contain 'codigo_tienda'. Skipping...")
            continue

        topic_solicitudes = f"{codigo_tienda}-solicitudes"
        topic_despacho = f"{codigo_tienda}-despacho"
        items = orden_compra.get('list', [])
        fecha_solicitud = datetime.now().isoformat()

        products = load_data(data_file)

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
                    print(response)
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
                    print(response)
                    send_to_kafka(topic_solicitudes, response)
            else:
                response = {
                    'estado': 'RECHAZADA',
                    'observaciones': f'Producto: {producto_code} no existe.',
                    'fecha_solicitud': fecha_solicitud
                }
                send_to_kafka(topic_solicitudes, response)


class ValidationError:
    def __init__(self, line_number, error_message):
        self.line_number = line_number
        self.error_message = error_message

    def to_dict(self):
        return {
            'linea': self.line_number,
            'error': self.error_message
        }


def validar_linea_info(linea):
    return all(field.strip() for field in linea)


def check_username_exists(connection, username):
    """Verificar si el usuario ya existe"""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) as count FROM users WHERE username = %s", (username,))
        result = cursor.fetchone()
        return result['count'] > 0


def check_tienda_status(connection, store_code):
    """Verificar si la tienda existe y está activa"""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id, enabled FROM stores WHERE code = %s", (store_code,))
        store = cursor.fetchone()

        if not store:
            return None, f"Tienda con código {store_code} no existe"
        if not store['enabled']:
            return None, f"Tienda con código {store_code} está deshabilitada"

        return store['id'], None


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'stockearte',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}


def get_db_connection():
    """Crear conexión a la base de datos usando pymysql"""
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Error al crear conexión a la base de datos: {e}")
        raise


@app.route('/users/bulk-upload', methods=['POST'])
def bulk_upload_users():
    if 'file' not in request.files:
        return jsonify({'error': 'No se proporcionó archivo'}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'El archivo debe ser CSV'}), 400

    try:
        # Leer el archivo CSV
        csv_content = file.read().decode('utf-8')
        csv_file = io.StringIO(csv_content)
        csv_reader = csv.reader(csv_file, delimiter=';')

        validation_errors = []
        created_users = []
        line_number = 0

        connection = get_db_connection()

        try:
            for row in csv_reader:
                line_number += 1

                # Validar número de campos
                if len(row) != 5:
                    validation_errors.append(
                        ValidationError(
                            line_number, "Número incorrecto de campos")
                    )
                    continue

                username, password, first_name, last_name, store_code = row

                # Validar campos vacíos
                if not validar_linea_info(row):
                    validation_errors.append(
                        ValidationError(
                            line_number, "Campos vacíos no permitidos")
                    )
                    continue

                # Verificar duplicidad de usuario
                if check_username_exists(connection, username):
                    validation_errors.append(
                        ValidationError(
                            line_number, f"Usuario {username} ya existe")
                    )
                    continue

                # Verificar tienda
                store_id, store_error = check_tienda_status(
                    connection, store_code)
                if store_error:
                    validation_errors.append(
                        ValidationError(line_number, store_error)
                    )
                    continue

                try:
                    # Insertar nuevo usuario
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO users (username, password, first_name, last_name, enabled, store_id)
                            VALUES (%s, %s, %s, %s, 1, %s)
                        """, (username, password, first_name, last_name, store_id))

                    connection.commit()

                    created_users.append({
                        'username': username,
                        'first_name': first_name,
                        'last_name': last_name,
                        'store_code': store_code
                    })

                except Exception as e:
                    connection.rollback()
                    validation_errors.append(
                        ValidationError(
                            line_number, f"Error al insertar usuario: {str(e)}")
                    )
                    continue

        finally:
            connection.close()

        # Preparar respuesta
        response = {
            'usuarios_creados': len(created_users),
            'usuarios': created_users,
            'errores': [error.to_dict() for error in validation_errors]
        }

        return jsonify(response)

    except Exception as e:
        logging.error(f"Error al procesar archivo CSV: {str(e)}")
        return jsonify({'error': f'Error al procesar archivo: {str(e)}'}), 500


# Iniciar el consumo de mensajes en un hilo separado
kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
kafka_thread.start()


def inicializador_de_ordenes():
    """Inicializar el archivo orders.json con datos de ejemplo"""
    try:
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
            logging.info(
                "orders.json se inicializó con información de ejemplo correctamente")
        else:
            if os.path.getsize(orders_file) == 0:
                logging.warning(
                    "orders.json existe pero está vacío. Inicializando con datos de ejemplo.")
                save_data([], orders_file)
            else:
                logging.info("orders.json ya existe y contiene datos")
    except Exception as e:
        logging.error(f"Error al inicializar orders.json: {str(e)}")


if __name__ == '__main__':
    inicializador_de_ordenes()
    app.run(debug=True, port=5000)
