class ProductManager:
    def __init__(self, products_yaml: dict):
        self.products = products_yaml.get('produtos', {})
    
    def get_product(self, product_id: str):
        return self.products.get(product_id, {})