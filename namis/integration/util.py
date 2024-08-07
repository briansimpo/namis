
class JsonObject:

    def __init__(self, dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                self.__dict__[key] = JsonObject(value)
            elif isinstance(value, list):
                self.__dict__[key] = [JsonObject(item) if isinstance(item, dict) else item for item in value]
            else:
                self.__dict__[key] = value

    def __getattr__(self, name):
        return self.__dict__.get(name)



def to_bool(value):
    if isinstance(value, str):
        value = value.strip().lower() 
        if value == "yes":
            return True
        elif value == "no":
            return False
        else:
            return False
        

def get_message():
    return "File uploaded. You will be nofified when the process is completed"