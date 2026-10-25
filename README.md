# Online Food Delivery Partner (Producer-Consumer Problem)

OS practical on process synchronization.

- Producer: Restaurant, adds ready parcels to a bounded buffer
- Consumer: Delivery partners, pick parcels
- Race condition: fixed using a lock (synchronized in Java, Condition in Python)
- Buffer full: new order is cancelled
- Buffer empty: delivery partner waits

## Run
- Visual demo: open food_delivery.html in a browser
