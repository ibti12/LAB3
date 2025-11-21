import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Generate 1000 Customers
def generate_customers(num_customers=1000):
    customers = []
    segments = ['Enterprise', 'SMB', 'Startup', 'Individual']
    
    for i in range(1, num_customers + 1):
        customer = {
            'customerNumber': i,
            'customerName': fake.company(),
            'contactFirstName': fake.first_name(),
            'contactLastName': fake.last_name(),
            'phone': fake.phone_number(),
            'addressLine1': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'country': fake.country(),
            'creditLimit': round(random.uniform(5000, 100000), 2),
            'customerSegment': random.choice(segments)
        }
        customers.append(customer)
    
    with open('customers.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
    
    print(f"✓ Generated {num_customers} customers in customers.csv")

# Generate 100 Products
def generate_products(num_products=100):
    products = []
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
    
    for i in range(1, num_products + 1):
        buy_price = round(random.uniform(10, 500), 2)
        msrp = round(buy_price * random.uniform(1.2, 2.5), 2)
        
        product = {
            'productCode': f'PROD{i:04d}',
            'productName': fake.catch_phrase(),
            'productCategory': random.choice(categories),
            'quantityInStock': random.randint(0, 1000),
            'buyPrice': buy_price,
            'MSRP': msrp
        }
        products.append(product)
    
    with open('products.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    
    print(f"✓ Generated {num_products} products in products.csv")

# Generate 5000 Orders
def generate_orders(num_orders=5000):
    orders = []
    statuses = ['Shipped', 'Processing', 'Delivered', 'Cancelled', 'Pending']
    payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Debit Card']
    
    start_date = datetime(2023, 1, 1)
    
    for i in range(1, num_orders + 1):
        order_date = start_date + timedelta(days=random.randint(0, 730))
        required_date = order_date + timedelta(days=random.randint(3, 14))
        
        order = {
            'orderNumber': i,
            'orderDate': order_date.strftime('%Y-%m-%d'),
            'requiredDate': required_date.strftime('%Y-%m-%d'),
            'status': random.choice(statuses),
            'customerNumber': random.randint(1, 1000),
            'totalAmount': round(random.uniform(50, 5000), 2),
            'paymentMethod': random.choice(payment_methods)
        }
        orders.append(order)
    
    with open('orders.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✓ Generated {num_orders} orders in orders.csv")

if __name__ == "__main__":
    print("Generating e-commerce dataset...")
    generate_customers(1000)
    generate_products(100)
    generate_orders(5000)
    print("\n✓ All datasets generated successfully!")
    print("\nNext steps:")
    print("1. Verify files with: head customers.csv")
    print("2. Run the Spark exploration script")