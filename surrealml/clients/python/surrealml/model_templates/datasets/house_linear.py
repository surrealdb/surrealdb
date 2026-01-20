import numpy as np


raw_squarefoot = np.array([1000, 1200, 1500, 1800, 2000, 2200, 2500, 2800, 3000, 3200], dtype=np.float32)
raw_num_floors = np.array([1, 1, 1.5, 1.5, 2, 2, 2.5, 2.5, 3, 3], dtype=np.float32)
raw_house_price = np.array([200000, 230000, 280000, 320000, 350000, 380000, 420000, 470000, 500000, 520000],
                               dtype=np.float32)
squarefoot = (raw_squarefoot - raw_squarefoot.mean()) / raw_squarefoot.std()
num_floors = (raw_num_floors - raw_num_floors.mean()) / raw_num_floors.std()
house_price = (raw_house_price - raw_house_price.mean()) / raw_house_price.std()
inputs = np.column_stack((squarefoot, num_floors))


HOUSE_LINEAR = {
    "inputs": inputs,
    "outputs": house_price,

    "squarefoot": squarefoot,
    "num_floors": num_floors,
    "input order": ["squarefoot", "num_floors"],
    "raw_inputs": {
        "squarefoot": raw_squarefoot,
        "num_floors": raw_num_floors,
    },
    "normalised_inputs": {
        "squarefoot": squarefoot,
        "num_floors": num_floors,
    },
    "normalisers": {
        "squarefoot": {
            "type": "z_score",
            "mean": squarefoot.mean(),
            "std": squarefoot.std()
        },
        "num_floors": {
            "type": "z_score",
            "mean": num_floors.mean(),
            "std": num_floors.std()
        }
    },
}
