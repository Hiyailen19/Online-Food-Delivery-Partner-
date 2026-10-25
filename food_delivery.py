"""
Online Food Delivery Partner - Producer-Consumer (Bounded Buffer)

Producer : Restaurant        -> adds ready parcels
Consumer : Delivery Partners -> pick ready parcels

Conditions handled:
1. Race condition : count is updated only while holding the lock.
2. Buffer full    : new order is CANCELLED (producer never waits).
3. Buffer empty   : delivery partner WAITS until a parcel is ready.
"""

import threading
import time


class ParcelBuffer:
    """Thread-safe bounded buffer holding the count of ready parcels."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._count = 0
        self._closed = False
        self._condition = threading.Condition()  # lock + wait/notify

    def add_parcel(self, order_id: int) -> bool:
        """Producer: add a parcel, or cancel the order if the buffer is full."""
        with self._condition:
            if self._count == self._capacity:
                print(f"Restaurant : Order #{order_id} CANCELLED "
                      f"(buffer full, ready = {self._count})")
                return False

            self._count += 1
            print(f"Restaurant : Order #{order_id} is READY. "
                  f"Ready parcels = {self._count}")
            self._condition.notify_all()  # wake waiting partners
            return True

    def take_parcel(self, partner_name: str) -> bool:
        """Consumer: take a parcel, waiting while the buffer is empty.

        Returns False when the restaurant has closed and nothing is left.
        """
        with self._condition:
            while self._count == 0:
                if self._closed:
                    return False
                print(f"{partner_name} : No parcel, WAITING...")
                self._condition.wait()  # releases lock while waiting

            self._count -= 1
            print(f"{partner_name} : Picked a parcel. "
                  f"Ready parcels = {self._count}")
            return True

    def close(self) -> None:
        """Called by the restaurant when it has placed all its orders."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()  # release partners still waiting


def restaurant(buffer: ParcelBuffer, total_orders: int, prep_time: float) -> None:
    """Producer thread."""
    for order_id in range(1, total_orders + 1):
        buffer.add_parcel(order_id)
        time.sleep(prep_time)
    buffer.close()


def delivery_partner(name: str, buffer: ParcelBuffer, delivery_time: float) -> None:
    """Consumer thread."""
    while buffer.take_parcel(name):
        time.sleep(delivery_time)  # time taken to deliver
    print(f"{name} : No more orders. Going OFFLINE.")


def main() -> None:
    buffer = ParcelBuffer(capacity=3)  # limited buffer size

    threads = [
        threading.Thread(target=restaurant, args=(buffer, 10, 0.3), name="Restaurant"),
        threading.Thread(target=delivery_partner, args=("Partner-1", buffer, 0.8)),
        threading.Thread(target=delivery_partner, args=("Partner-2", buffer, 0.8)),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()  # wait for everything to finish

    print("All orders processed.")


if __name__ == "__main__":
    main()
