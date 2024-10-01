from datetime import datetime
import json
import os
import random
import threading
import logging
from kafka import KafkaConsumer, KafkaProducer
from flask import Flask, request, jsonify
from flask_cors import CORS

# Flask setup
app = Flask(__name__)
data_file = 'products.json'
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

# Helper function to load data from JSON file
def load_data():
    if os.path.exists(data_file):
        with open(data_file, 'r') as f:
            return json.load(f)
    return []

# Helper function to save data to JSON file
def save_data(data):
    with open(data_file, 'w') as f:
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

# Flask Routes

@app.route('/products', methods=['POST'])
def create_product():
    products = load_data()
    new_product = request.json
    new_product['id'] = (products[-1]['id'] + 1) if products else 1
    new_product['code'] = generate_code()

    # Add new product to the list and save
    products.append(new_product)
    save_data(products)

    return jsonify(new_product), 201

# Get all products
@app.route('/products', methods=['GET'])
def get_products():
    products = load_data()
    return jsonify(products)

# Get a product by ID
@app.route('/products/<int:id>', methods=['GET'])
def get_product(id):
    products = load_data()
    product = next((p for p in products if p['id'] == id), None)
    if product:
        return jsonify(product)
    return jsonify({'error': 'Product not found'}), 404

# Update a product by ID
@app.route('/products/<int:id>', methods=['PUT'])
def update_product(id):
    products = load_data()
    product = next((p for p in products if p['id'] == id), None)
    if product:
        # Update product fields with request data
        updated_data = request.json
        product.update(updated_data)
        save_data(products)
        return jsonify(product)
    return jsonify({'error': 'Product not found'}), 404

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
