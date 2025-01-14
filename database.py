class Database:
    def __init__(self, node):
        self.store = {}
        self.node = node

    def set(self, key, value):
        self.store[key] = value
        self.node.sync_data()
        return f"SUCCESS: {key} -> {value} added."

    def get(self, key):
        if key in self.store:
            return f"{key} -> {self.store[key]}"
        return "ERROR: Key not found."

    def update(self, key, value):
        if key in self.store:
            self.store[key] = value
            return f"SUCCESS: {key} updated to {value}."
        self.node.sync_data()
        return "ERROR: Key not found."

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            self.node.sync_delete(key)
            return f"SUCCESS: {key} removed."
        return "ERROR: Key not found."

    def status(self):
        keys = ", ".join(self.store.keys())
        return f"Database keys: {keys}" if keys else "Database is empty."
