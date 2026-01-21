import os
import shutil
from unittest import TestCase

import numpy as np

from surrealml import Engine, SurMlFile
from surrealml.model_templates.torch.torch_linear import train_model


class TestSurMlFile(TestCase):

    def setUp(self):
        self.squarefoot = np.array([1000, 1200, 1500, 1800, 2000, 2200, 2500, 2800, 3000, 3200], dtype=np.float32)
        self.num_floors = np.array([1, 1, 1.5, 1.5, 2, 2, 2.5, 2.5, 3, 3], dtype=np.float32)
        self.house_price = np.array([200000, 230000, 280000, 320000, 350000, 380000, 420000, 470000, 500000, 520000],
                                            dtype=np.float32)
        self.model, self.x = train_model()
        self.file = SurMlFile(model=self.model, name="House Price Prediction", inputs=self.x[:1], engine=Engine.PYTORCH)

    def tearDown(self):
        try:
            shutil.rmtree(".surmlcache")
        except OSError as e:
            print(f"Error: surmlcache : {e.strerror}")
        os.remove("./test.surml")

    def test_full_torch_run(self):
        self.file.add_column("squarefoot")
        self.file.add_column("num_floors")

        self.file.add_output(
            "house_price",
            "z_score",
            self.house_price.mean(),
            self.house_price.std()
        )
        self.file.add_normaliser(
            "squarefoot",
            "z_score",
            self.squarefoot.mean(),
            self.squarefoot.std()
        )
        self.file.add_normaliser(
            "num_floors",
            "z_score",
            self.num_floors.mean(),
            self.num_floors.std()
        )
        self.file.add_description(description="Model that predicts the price of a house")
        self.file.add_version(version="1.0.0")

        self.file.save("./test.surml")

        new_file = SurMlFile.load("./test.surml", Engine.PYTORCH)

        self.assertEqual(float, type(new_file.raw_compute([1.0, 2.0])[0]))
        self.assertEqual(float, type(new_file.buffered_compute({"squarefoot": 1.0, "num_floors": 2.0})[0]))
        self.assertEqual("House Price Prediction", new_file.name)
        self.assertEqual("Model that predicts the price of a house", new_file.description)
        self.assertEqual("1.0.0", new_file.version)
        self.assertEqual(Engine.PYTORCH, new_file.engine)
