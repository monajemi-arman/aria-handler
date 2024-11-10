# aria-handler
Aria pharmacy accounting software handler  
The purpose of this project is too provide an API for Aria pharmacy accounting software that is used in some pharmacies in Iran.  
The program itself does not provide a method to manage products remotely.  
This project fills this gap. You only need access to the main Microsoft SQL server and valid credentials.

# Installation
* Clone this repository
```bash
git clone https://github.com/monajemi-arman/aria-handler
```
* Install as python module
```bash
cd aria-handler
python -m pip install -e .
```

# Usage
* Prepare **_config.json_** according to template in _'config.json.default'_.
* Import handler
```python
from aria_handler import AriaHandler

ah = AriaHandler('config.json')
# Now it tries to connect to the sql server
```
By default, it tries to keep the connection alive after first successful try.
* Add new noskhe with products (prescription)
```python
ah.add_products_to_noskhe(
    ((product_id, amount), )
) # Single product in one prescription (don't forget the comma to make the one member tuple, a tuple)
ah.add_products_to_noskhe(
    ((product_id, amount), (product_id, amount), ...)
) # For multiple products in one prescription
```
* Other functionalities included:
  * Get item price
```python
ah.get_price(product_id)  # -> price number
```
  * Get available amount of a product
```python
ah.get_stock(product_id)  # -> amount number
```
  * Convert user to id
```python
ah.user_to_id(user)  # -> id
```
  * and more!