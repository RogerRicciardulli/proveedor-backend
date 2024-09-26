import json
import os
import random
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
data_file = 'products.json'
CORS(app)  # This will allow all cross-origin requests

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

# Generate a 10-digit random code
def generate_code():
    return str(random.randint(1000000000, 9999999999))

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
