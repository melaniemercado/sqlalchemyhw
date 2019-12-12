from sqlalchemy import Column, Integer, String, Numeric, ForeignKey
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import desc, func, cast, Date, distinct, union, DateTime, text, join, update
from sqlalchemy import or_, and_, not_
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from pprint import pprint

engine = create_engine('sqlite:////web/sqlite-data/example.db')

Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer(), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(50), nullable=False)
    email = Column(String(200), nullable=False)
    address = Column(String(200), nullable=False)
    town = Column(String(50), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    orders = relationship("Order", backref='customer')


class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer(), primary_key=True)
    name = Column(String(200), nullable=False)
    cost_price = Column(Numeric(10, 2), nullable=False)
    selling_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer(), nullable=False)


class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer(), ForeignKey('customers.id'))
    date_placed = Column(DateTime(), default=datetime.now, nullable=False)
    date_shipped = Column(DateTime())


class OrderLine(Base):
    __tablename__ = 'order_lines'
    id = Column(Integer(), primary_key=True)
    order_id = Column(Integer(), ForeignKey('orders.id'))
    item_id = Column(Integer(), ForeignKey('items.id'))
    quantity = Column(Integer())
    order = relationship("Order", backref='order_lines')
    item = relationship("Item")

# function to check valid order_id
def dispatch_order(order_id):
    order = session.query(Order).get(order_id)

    if not order:
        raise ValueError("Invalid order id: {}.".format(order_id))

    if order.date_shipped:
        print("Order already shipped.")
        return

    try:
        for i in order.order_lines:
            i.item.quantity = i.item.quantity - i.quantity

        order.date_shipped = datetime.now()
        session.commit()
        print("Transaction completed.")

    except IntegrityError as e:
        print(e)
        print("Rolling back ...")
        session.rollback()
        print("Transaction failed.")


Base.metadata.create_all(engine)

# Creating Session
# from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

# Inserting Data
c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )

session.add(c1)
session.add(c2)
session.new
session.commit()

# Inserting  More Data
c3 = Customer(
    first_name="John",
    last_name="Lara",
    username="johnlara",
    email="johnlara@mail.com",
    address="3073 Derek Drive",
    town="Norfolk"
)

c4 = Customer(
    first_name="Sarah",
    last_name="Tomlin",
    username="sarahtomlin",
    email="sarahtomlin@mail.com",
    address="3572 Poplar Avenue",
    town="Norfolk"
)

c5 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c6 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )

session.add_all([c3, c4, c5, c6])
session.commit()

# Add products
i1 = Item(name = 'Chair', cost_price = 9.21, selling_price = 10.81, quantity = 5)
i2 = Item(name = 'Pen', cost_price = 3.45, selling_price = 4.51, quantity = 3)
i3 = Item(name = 'Headphone', cost_price = 15.52, selling_price = 16.81, quantity = 50)
i4 = Item(name = 'Travel Bag', cost_price = 20.1, selling_price = 24.21, quantity = 50)
i5 = Item(name = 'Keyboard', cost_price = 20.1, selling_price = 22.11, quantity = 50)
i6 = Item(name = 'Monitor', cost_price = 200.14, selling_price = 212.89, quantity = 50)
i7 = Item(name = 'Watch', cost_price = 100.58, selling_price = 104.41, quantity = 50)
i8 = Item(name = 'Water Bottle', cost_price = 20.89, selling_price = 25, quantity = 50)

session.add_all([i1, i2, i3, i4, i5, i6, i7, i8])
session.commit()

# Create orders
o1 = Order(customer=c1)
o2 = Order(customer=c1)

line_item1 = OrderLine(order=o1, item=i1, quantity=3)
line_item2 = OrderLine(order=o1, item=i2, quantity=2)
line_item3 = OrderLine(order=o2, item=i1, quantity=1)
line_item3 = OrderLine(order=o2, item=i2, quantity=4)

session.add_all([o1, o2])
session.new
session.commit()

o3 = Order(customer=c1)
orderline1 = OrderLine(item=i1, quantity=5)
orderline2 = OrderLine(item=i2, quantity=10)

o3.order_lines.append(orderline1)
o3.order_lines.append(orderline2)

session.add_all([o3])
session.commit()

# Querying Database

# all() method
session.query(Customer).all()
print("session.query(Customer).all()=", session.query(Customer).all())

print("session.query(Item).all() session.query(Order).all()=", session.query(Item).all(), session.query(Order).all())

print(session.query(Customer))

q = session.query(Customer)

for c in q:
    print(c.id, c.first_name)

print("session.query(Customer.id, Customer.first_name).all()=", session.query(Customer.id, Customer.first_name).all())
session.query(Customer).count() # get the total number of records in the customers table
session.query(Item).count()  # get the total number of records in the items table
session.query(Order).count()  # get the total number of records in the orders table

# count() method
print("session.query(Customer).count()", session.query(Customer).count() ) # get the total number of records in the customers table
print("session.query(Item).count()", session.query(Item).count())  # get the total number of records in the items table
print("session.query(Order).count()", session.query(Order).count())  # get the total number of records in the orders table

# get() method
print("session.query(Customer).get(1)",session.query(Customer).get(1))
print("session.query(Item).get(1)", session.query(Item).get(1))
print("session.query(Order).get(100)" , session.query(Order).get(100))


#filter() method
print("session.query(Customer).filter(Customer.first_name == 'John').all()",session.query(Customer).filter(Customer.first_name == 'John').all())

print(session.query(Customer).filter(Customer.first_name == 'John'))

print("session.query(Customer).filter(Customer.id <= 7, Customer.town == 'Norfolk').all()", session.query(Customer).filter(Customer.id <= 7, Customer.town == 'Norfolk').all())


print(session.query(Customer).filter(Customer.id <= 7, Customer.town.like("Nor%")))

print("-----------")
from sqlalchemy import or_, and_, not_

# find all customers who either live in Peterbrugh or Norfolk

print(session.query(Customer).filter(or_(
    Customer.town == 'Peterbrugh',
    Customer.town == 'Norfolk'
)).all())

# find all customers whose first name is John and live in Norfolk

print(session.query(Customer).filter(and_(
    Customer.first_name == 'John',
    Customer.town == 'Norfolk'
)).all())

# Customers with first name John
output = session.query(Customer).filter(text("first_name = 'John'")).all()
print("Find all Customers with first name John")
for i in output:
    print("Name: ", i.first_name, " ", i.last_name, " Address:", i.address, " Email:", i.email)

# Customers with town starting with Nor
session.query(Customer).filter(text("town like 'Nor%'")).all()
print("Find all Customers with town starting with Nor")
for i in output:
    print("Name: ", i.first_name, " ", i.last_name, " Address:", i.address, " Email:", i.email)

#Dealing with Duplicates
from sqlalchemy import distinct
print(session.query(Customer.town).filter(Customer.id  < 10).all())
print(session.query(Customer.town).filter(Customer.id  < 10).distinct().all())

print(session.query(
    func.count(distinct(Customer.town)),
    func.count(Customer.town)
).all())

#Casting
from sqlalchemy import cast, Date, distinct, union
session.query(
    cast(func.pi(), Integer),
    cast(func.pi(), Numeric(10, 2)),
    cast("2010-12-01", DateTime),
    cast("2010-12-01", Date),
all()
)


# Unions
s1 = session.query(Item.id, Item.name).filter(Item.name.like("Wa%"))
s2 = session.query(Item.id, Item.name).filter(Item.name.like("%e%"))
print(s1.union(s2).all())


# Union All
print(s1.union_all(s2).all())

# Updating Data
i = session.query(Item).get(8)
i.selling_price = 25.91

session.add(i)
session.commit()

# update quantity of all quantity of items to 60 whose name starts with 'W'

session.query(Item).filter(
    Item.name.ilike("W%")
).update({"quantity": 60}, synchronize_session='fetch')
session.commit()


# Deleting data
#i = session.query(Item).filter(Item.name == 'Monitor').one()
#i
#session.delete(i)
#session.commit()

# To delete multiple records at once use the delete() method of the Query object.

session.query(Item).filter(
    Item.name.ilike("W%")
).delete(synchronize_session='fetch')
session.commit()


#  Raw Queries

from sqlalchemy import text

print(session.query(Customer).filter(text("first_name = 'John'")).all())

print(session.query(Customer).filter(text("town like 'Nor%'")).all())

print(session.query(Customer).filter(text("town like 'Nor%'")).order_by(text("first_name, id desc")).all())


# Transactions
def dispatch_order(order_id):
    # check whether order_id is valid or not
    order = session.query(Order).get(order_id)

    if not order:
        raise ValueError("Invalid order id: {}.".format(order_id))

    if order.date_shipped:
        pprint("Order already shipped.")
        return

    try:
        for i in order.order_lines:
            i.item.quantity = i.item.quantity - i.quantity

        order.date_shipped = datetime.now()
        session.commit()
        pprint("Transaction completed.")

    except IntegrityError as e:
        pprint(e)
        pprint("Rolling back ...")
        session.rollback()
        pprint("Transaction failed.")


dispatch_order(1)
session.commit()
