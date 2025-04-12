class ProductManager:
    def __init__(self, products_yaml: dict):
        self.products = products_yaml.get('produtos', {})
    
    def get_product(self, product_id: str):
        return self.products.get(product_id, {})
    
    def get_products(self):
        return {pid: pinfo["config"] for pid, pinfo in self.products.items()}
    
    def get_task_by_color(self, color: str):
        color_to_task = {
            'Yellow': 'T1A',
            'Blue': 'T1B',
            'Green': 'T1C',
            'Orange': 'T1D',
            'Red': 'T1E'
        }
        return color_to_task.get(color)
    