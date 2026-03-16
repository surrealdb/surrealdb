from unittest import TestCase, main
from surrealml.model_templates.torch.torch_linear import train_model
from surrealml.rust_adapter import RustAdapter
from surrealml.surml_file import SurMlFile
from surrealml.engine import Engine
import shutil


class TestRustAdapter(TestCase):

    def setUp(self):
        self.model, self.x = train_model()
        self.file = SurMlFile(model=self.model, name="linear", inputs=self.x, engine=Engine.PYTORCH)

    def tearDown(self):
        shutil.rmtree(".surmlcache")

    def test_basic_store(self):
        # pass
        self.file.add_column(name="x")
        # self.file.save(path="./unit_test.surml")


if __name__ == '__main__':
    main()
