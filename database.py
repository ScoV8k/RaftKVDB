class Database:
    def __init__(self):
        self.store = {}
        self.log = []
        self.commit_index = -1

    def append_log(self, operation):
        self.log.append(operation)
        return len(self.log) - 1 

    def apply_log_entry(self, entry):
        operation = entry["operation"]
        key = entry["key"]
        value = entry.get("value")

        if operation == "SET":
            self.store[key] = value
            return f"SUCCESS: {key} -> {value} added."
        elif operation == "UPDATE":
            if key in self.store:
                self.store[key] = value
                return f"SUCCESS: {key} updated to {value}."
            return "ERROR: Key not found."
        elif operation == "DELETE":
            if key in self.store:
                del self.store[key]
                return f"SUCCESS: {key} removed."
            return "ERROR: Key not found."


    def commit_log_entries(self, commit_index):
        result = None
        while self.commit_index < commit_index:
            self.commit_index += 1
            if self.commit_index < len(self.log):
                result = self.apply_log_entry(self.log[self.commit_index])
        return result

    # def set(self, key, value):
    #     self.store[key] = value
    #     return f"SUCCESS: {key} -> {value} added."

    def get(self, key):
        if key in self.store:
            return f"{key} -> {self.store[key]}"
        return "ERROR: Key not found."

    # def update(self, key, value):
    #     if key in self.store:
    #         self.store[key] = value
    #         return f"SUCCESS: {key} updated to {value}."
    #     return "ERROR: Key not found."

    # def delete(self, key):
    #     if key in self.store:
    #         del self.store[key]
    #         return f"SUCCESS: {key} removed."
    #     return "ERROR: Key not found."

    def status(self):
        keys = ", ".join(self.store.keys())
        return f"Database keys: {keys}" if keys else "Database is empty."
