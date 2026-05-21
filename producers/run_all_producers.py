import threading

from producers.order_event_producer import main as order_producer
from producers.payment_event_producer import main as payment_producer
from producers.inventory_event_producer import main as inventory_producer
from producers.delivery_event_producer import main as delivery_producer
from producers.cart_event_producer import main as cart_producer
from producers.customer_activity_producer import (main as customer_activity_producer)
from producers.product_view_event_producer import (main as product_view_producer)

def start_order_producer():
    order_producer()

def start_payment_producer():
    payment_producer()

def start_inventory_producer():
    inventory_producer()

def start_delivery_producer():
    delivery_producer()

def start_cart_producer():
    cart_producer()

def start_customer_activity_producer():
    customer_activity_producer()

def start_product_view_producer():
    product_view_producer()

if __name__ == "__main__":
    threads = [
        threading.Thread(target=start_order_producer),
        threading.Thread(target=start_payment_producer),
        threading.Thread(target=start_inventory_producer),
        threading.Thread(target=start_delivery_producer),
        threading.Thread(target=start_cart_producer),
        threading.Thread(target=start_customer_activity_producer),
        threading.Thread(target=start_product_view_producer)
    ]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()