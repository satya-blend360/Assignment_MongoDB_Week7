"""
MongoDB Sales Data Storage and Query System
Demonstrates CRUD operations and aggregation pipeline on sales data
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime
import csv
from pprint import pprint

class SalesDataManager:
    def __init__(self, connection_string, db_password):
        """Initialize MongoDB connection"""
        # Replace <db_password> with actual password
        conn_str = connection_string.replace('<db_password>', db_password)
        
        try:
            self.client = MongoClient(conn_str)
            # Test connection
            self.client.admin.command('ping')
            print("✓ Successfully connected to MongoDB!")
            
            self.db = self.client['sales_database']
            self.orders_collection = self.db['orders']
            print(f"✓ Using database: sales_database")
            print(f"✓ Using collection: orders\n")
            
        except ConnectionFailure as e:
            print(f"✗ Failed to connect to MongoDB: {e}")
            raise
    
    def clear_collection(self):
        """Clear all documents from the collection"""
        result = self.orders_collection.delete_many({})
        print(f"Cleared {result.deleted_count} existing documents\n")
    
    def load_csv_data(self, csv_file_path):
        """Load CSV data and convert to nested JSON documents"""
        print("Loading CSV data...")
        documents = []
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    # Convert CSV row to nested JSON document
                    document = {
                        'order_id': row['Order ID'],
                        'date': datetime.strptime(row['Date'], '%Y-%m-%d'),
                        'status': row['Status'],
                        'fulfilment': row['Fulfilment'],
                        'sales_channel': row['Sales Channel'],
                        
                        # Customer information (nested)
                        'customer': {
                            'b2b': row['B2B'] == 'True',
                            'shipping': {
                                'city': row['ship-city'],
                                'state': row['ship-state'],
                                'postal_code': row['ship-postal-code'],
                                'country': row['ship-country']
                            }
                        },
                        
                        # Product information (nested)
                        'product': {
                            'style': row['Style'],
                            'sku': row['SKU'],
                            'category': row['Category'],
                            'size': row['Size'],
                            'asin': row['ASIN'],
                            'quantity': int(row['Qty']) if row['Qty'] else 0
                        },
                        
                        # Order details
                        'order_details': {
                            'service_level': row['ship-service-level'],
                            'courier_status': row['Courier Status'],
                            'fulfilled_by': row['fulfilled-by'],
                            'promotions': row['promotion-ids'] if row['promotion-ids'] != 'No Promotion' else None
                        },
                        
                        # Financial information
                        'financial': {
                            'currency': row['currency'],
                            'amount': float(row['Amount']) if row['Amount'] else 0.0
                        },
                        
                        # Date breakdown for easier querying
                        'date_info': {
                            'year': int(row['Year']),
                            'month': int(row['Month']),
                            'month_name': row['MonthName'],
                            'day': int(row['Day'])
                        }
                    }
                    documents.append(document)
            
            if documents:
                result = self.orders_collection.insert_many(documents)
                print(f"✓ Inserted {len(result.inserted_ids)} documents into MongoDB\n")
                return len(result.inserted_ids)
            else:
                print("✗ No documents to insert\n")
                return 0
                
        except FileNotFoundError:
            print(f"✗ Error: CSV file not found at {csv_file_path}")
            print("Please ensure 'Cleaned_Amazon_Sale_Report.csv' is in the same directory\n")
            return 0
        except Exception as e:
            print(f"✗ Error loading CSV: {e}\n")
            return 0
    
    # ============= CRUD OPERATIONS =============
    
    def create_order(self, order_data):
        """CREATE: Insert a new order"""
        print("\n--- CREATE Operation ---")
        result = self.orders_collection.insert_one(order_data)
        print(f"✓ Created order with ID: {result.inserted_id}")
        return result.inserted_id
    
    def read_order(self, order_id):
        """READ: Retrieve a specific order"""
        print(f"\n--- READ Operation ---")
        order = self.orders_collection.find_one({'order_id': order_id})
        if order:
            print(f"✓ Found order: {order_id}")
            pprint(order)
        else:
            print(f"✗ Order not found: {order_id}")
        return order
    
    def update_order(self, order_id, update_data):
        """UPDATE: Update an existing order"""
        print(f"\n--- UPDATE Operation ---")
        result = self.orders_collection.update_one(
            {'order_id': order_id},
            {'$set': update_data}
        )
        print(f"✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
        return result.modified_count
    
    def delete_order(self, order_id):
        """DELETE: Remove an order"""
        print(f"\n--- DELETE Operation ---")
        result = self.orders_collection.delete_one({'order_id': order_id})
        print(f"✓ Deleted {result.deleted_count} document(s)")
        return result.deleted_count
    
    # ============= QUERY OPERATIONS =============
    
    def query_orders_by_date_range(self, start_date, end_date):
        """Query 1: Retrieve all orders in a given date range"""
        print(f"\n{'='*60}")
        print(f"QUERY 1: Orders between {start_date} and {end_date}")
        print(f"{'='*60}")
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        orders = self.orders_collection.find({
            'date': {
                '$gte': start,
                '$lte': end
            }
        }).limit(5)  # Limit to 5 for display
        
        count = self.orders_collection.count_documents({
            'date': {'$gte': start, '$lte': end}
        })
        
        print(f"\n✓ Found {count} orders in date range")
        print("\nSample orders (first 5):")
        for i, order in enumerate(orders, 1):
            print(f"\n{i}. Order ID: {order['order_id']}")
            print(f"   Date: {order['date'].strftime('%Y-%m-%d')}")
            print(f"   Status: {order['status']}")
            print(f"   Amount: {order['financial']['currency']} {order['financial']['amount']}")
            print(f"   Region: {order['customer']['shipping']['state']}")
        
        return count
    
    def aggregate_sales_by_region(self):
        """Query 2: Group sales by region (state) using aggregation pipeline"""
        print(f"\n{'='*60}")
        print("QUERY 2: Sales Aggregation by Region (State)")
        print(f"{'='*60}")
        
        pipeline = [
            # Filter out cancelled orders
            {
                '$match': {
                    'status': {'$ne': 'Cancelled'}
                }
            },
            # Group by state
            {
                '$group': {
                    '_id': '$customer.shipping.state',
                    'total_sales': {'$sum': '$financial.amount'},
                    'order_count': {'$sum': 1},
                    'avg_order_value': {'$avg': '$financial.amount'},
                    'total_quantity': {'$sum': '$product.quantity'}
                }
            },
            # Sort by total sales descending
            {
                '$sort': {'total_sales': -1}
            },
            # Limit to top 10
            {
                '$limit': 10
            }
        ]
        
        results = list(self.orders_collection.aggregate(pipeline))
        
        print(f"\n✓ Top 10 Regions by Sales:\n")
        print(f"{'State':<20} {'Total Sales':>15} {'Orders':>10} {'Avg Order':>15} {'Qty':>10}")
        print("-" * 75)
        
        for result in results:
            state = result['_id'] if result['_id'] else 'Unknown'
            print(f"{state:<20} ₹{result['total_sales']:>14,.2f} {result['order_count']:>10} "
                  f"₹{result['avg_order_value']:>14,.2f} {result['total_quantity']:>10}")
        
        return results
    
    def aggregate_sales_by_category(self):
        """Query 3: Group sales by product category using aggregation pipeline"""
        print(f"\n{'='*60}")
        print("QUERY 3: Sales Aggregation by Product Category")
        print(f"{'='*60}")
        
        pipeline = [
            # Filter out cancelled orders with zero amount
            {
                '$match': {
                    'financial.amount': {'$gt': 0}
                }
            },
            # Group by category
            {
                '$group': {
                    '_id': '$product.category',
                    'total_revenue': {'$sum': '$financial.amount'},
                    'total_orders': {'$sum': 1},
                    'total_units': {'$sum': '$product.quantity'},
                    'avg_price': {'$avg': '$financial.amount'}
                }
            },
            # Sort by revenue descending
            {
                '$sort': {'total_revenue': -1}
            }
        ]
        
        results = list(self.orders_collection.aggregate(pipeline))
        
        print(f"\n✓ Sales by Category:\n")
        print(f"{'Category':<20} {'Revenue':>15} {'Orders':>10} {'Units':>10} {'Avg Price':>15}")
        print("-" * 75)
        
        for result in results:
            category = result['_id'] if result['_id'] else 'Unknown'
            print(f"{category:<20} ₹{result['total_revenue']:>14,.2f} {result['total_orders']:>10} "
                  f"{result['total_units']:>10} ₹{result['avg_price']:>14,.2f}")
        
        return results
    
    def aggregate_monthly_sales_trend(self):
        """Query 4: Monthly sales trend analysis"""
        print(f"\n{'='*60}")
        print("QUERY 4: Monthly Sales Trend")
        print(f"{'='*60}")
        
        pipeline = [
            {
                '$match': {
                    'financial.amount': {'$gt': 0}
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': '$date_info.year',
                        'month': '$date_info.month',
                        'month_name': '$date_info.month_name'
                    },
                    'total_sales': {'$sum': '$financial.amount'},
                    'order_count': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.year': 1,
                    '_id.month': 1
                }
            }
        ]
        
        results = list(self.orders_collection.aggregate(pipeline))
        
        print(f"\n✓ Monthly Sales Trend:\n")
        print(f"{'Month':<15} {'Total Sales':>15} {'Order Count':>15}")
        print("-" * 50)
        
        for result in results:
            month_year = f"{result['_id']['month_name']} {result['_id']['year']}"
            print(f"{month_year:<15} ₹{result['total_sales']:>14,.2f} {result['order_count']:>15}")
        
        return results
    
    def complex_aggregation_analysis(self):
        """Query 5: Complex multi-faceted analysis"""
        print(f"\n{'='*60}")
        print("QUERY 5: Complex Analysis - B2B vs B2C by Category")
        print(f"{'='*60}")
        
        pipeline = [
            {
                '$match': {
                    'financial.amount': {'$gt': 0}
                }
            },
            {
                '$group': {
                    '_id': {
                        'category': '$product.category',
                        'b2b': '$customer.b2b'
                    },
                    'total_revenue': {'$sum': '$financial.amount'},
                    'order_count': {'$sum': 1}
                }
            },
            {
                '$sort': {
                    '_id.category': 1,
                    '_id.b2b': -1
                }
            }
        ]
        
        results = list(self.orders_collection.aggregate(pipeline))
        
        print(f"\n✓ B2B vs B2C Analysis:\n")
        print(f"{'Category':<20} {'Type':>10} {'Revenue':>15} {'Orders':>10}")
        print("-" * 60)
        
        for result in results:
            category = result['_id']['category'] if result['_id']['category'] else 'Unknown'
            customer_type = 'B2B' if result['_id']['b2b'] else 'B2C'
            print(f"{category:<20} {customer_type:>10} ₹{result['total_revenue']:>14,.2f} {result['order_count']:>10}")
        
        return results
    
    def demonstrate_flexibility(self):
        """Demonstrate NoSQL flexibility vs SQL"""
        print(f"\n{'='*60}")
        print("NOSQL vs SQL: Flexibility Comparison")
        print(f"{'='*60}")
        
        print("\n✓ MongoDB (NoSQL) Advantages:")
        print("  1. Schema-less: Can store varied document structures")
        print("  2. Nested data: Customer, product info in single document")
        print("  3. No joins needed: All related data together")
        print("  4. Easy to add fields: Can add new fields without migration")
        print("  5. Horizontal scaling: Sharding for large datasets")
        
        print("\n✓ Example - Adding new field dynamically:")
        # Add a new field to one document without affecting others
        sample_order = self.orders_collection.find_one({'financial.amount': {'$gt': 0}})
        if sample_order:
            self.orders_collection.update_one(
                {'_id': sample_order['_id']},
                {'$set': {
                    'customer_satisfaction': {
                        'rating': 4.5,
                        'review': 'Great product!',
                        'reviewed_date': datetime.now()
                    }
                }}
            )
            print("  → Added 'customer_satisfaction' field to one document")
            print("  → No schema changes needed!")
            print("  → Other documents unaffected!")
        
        print("\n✓ SQL (Relational) Advantages:")
        print("  1. ACID compliance: Strong consistency guarantees")
        print("  2. Complex joins: Efficient for normalized data")
        print("  3. Mature ecosystem: Well-established tools")
        print("  4. Structured schema: Data integrity enforcement")
        print("  5. Standardized query language (SQL)")
        
        print("\n✓ When to use MongoDB:")
        print("  • Rapid development with changing requirements")
        print("  • Hierarchical/nested data structures")
        print("  • Large-scale data with horizontal scaling needs")
        print("  • Real-time analytics and aggregations")
        
        print("\n✓ When to use SQL:")
        print("  • Complex transactions requiring ACID")
        print("  • Highly normalized data with many relationships")
        print("  • Reporting with complex joins")
        print("  • Regulated industries requiring strict data integrity")
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("\n✓ MongoDB connection closed")


def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("MongoDB Sales Data Analysis System")
    print("="*60 + "\n")
    
    # Configuration
    CONNECTION_STRING = "mongodb+srv://saisreesatyassss_db_user:<db_password>@clusterweek7.9hva4vw.mongodb.net/"
    DB_PASSWORD = "saisreesatyassss_db_user"  # ← REPLACE WITH YOUR ACTUAL PASSWORD
    CSV_FILE = "Cleaned_Amazon_Sale_Report.csv"
    
    try:
        # Initialize manager
        manager = SalesDataManager(CONNECTION_STRING, DB_PASSWORD)
        
        # Clear existing data and load fresh data
        manager.clear_collection()
        count = manager.load_csv_data(CSV_FILE)
        
        if count > 0:
            # ===== CRUD DEMONSTRATIONS =====
            print("\n" + "="*60)
            print("CRUD OPERATIONS DEMONSTRATION")
            print("="*60)
            
            # CREATE
            new_order = {
                'order_id': 'TEST-123-456',
                'date': datetime(2022, 5, 1),
                'status': 'Pending',
                'customer': {
                    'b2b': False,
                    'shipping': {
                        'city': 'Hyderabad',
                        'state': 'Telangana',
                        'postal_code': '500001',
                        'country': 'IN'
                    }
                },
                'product': {
                    'category': 'Kurta',
                    'quantity': 2
                },
                'financial': {
                    'currency': 'INR',
                    'amount': 800.0
                }
            }
            manager.create_order(new_order)
            
            # READ
            manager.read_order('TEST-123-456')
            
            # UPDATE
            manager.update_order('TEST-123-456', {'status': 'Shipped'})
            
            # DELETE
            manager.delete_order('TEST-123-456')
            
            # ===== QUERY DEMONSTRATIONS =====
            print("\n" + "="*60)
            print("ADVANCED QUERY DEMONSTRATIONS")
            print("="*60)
            
            # Query 1: Date range
            manager.query_orders_by_date_range('2022-04-01', '2022-04-30')
            
            # Query 2: Sales by region
            manager.aggregate_sales_by_region()
            
            # Query 3: Sales by category
            manager.aggregate_sales_by_category()
            
            # Query 4: Monthly trend
            manager.aggregate_monthly_sales_trend()
            
            # Query 5: Complex analysis
            manager.complex_aggregation_analysis()
            
            # Flexibility comparison
            manager.demonstrate_flexibility()
            

        # Close connection
        manager.close()
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("1. Replace 'your_password_here' with your actual MongoDB password")
        print("2. Ensure 'Cleaned_Amazon_Sale_Report.csv' is in the same directory")
        print("3. Check your internet connection")
        print("4. Verify MongoDB Atlas cluster is running")


if __name__ == "__main__":
    main()