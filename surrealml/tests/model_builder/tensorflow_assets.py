from model_builder.utils import install_package
install_package("tf2onnx==1.16.1")
install_package("tensorflow==2.16.1")
import os

from surrealml.model_templates.tensorflow.tensorflow_linear import train_model as linear_tensorflow_train_model
from surrealml.model_templates.tensorflow.tensorflow_linear import export_model_onnx as linear_tensorflow_export_model_onnx
from surrealml.model_templates.tensorflow.tensorflow_linear import export_model_surml as linear_tensorflow_export_model_surml

from model_builder.utils import delete_directory, create_directory, MODEL_STASH_DIRECTORY


# create the model stash directory if it does not exist
create_directory(dir_path=MODEL_STASH_DIRECTORY)

tensorflow_stash_directory = os.path.join(MODEL_STASH_DIRECTORY, "tensorflow")
tensorflow_surml_stash_directory = os.path.join(tensorflow_stash_directory, "surml")
tensorflow_onnx_stash_directory = os.path.join(tensorflow_stash_directory, "onnx")

# delete the directories if they exist
delete_directory(dir_path=tensorflow_stash_directory)
delete_directory(dir_path=tensorflow_surml_stash_directory)
delete_directory(dir_path=tensorflow_onnx_stash_directory)

# create directories for the tensorflow models
os.mkdir(tensorflow_stash_directory)
os.mkdir(tensorflow_surml_stash_directory)
os.mkdir(tensorflow_onnx_stash_directory)

# train and stash tensorflow models
tensorflow_linear_model = linear_tensorflow_train_model()
tensorflow_linear_surml_file = linear_tensorflow_export_model_surml(tensorflow_linear_model)
tensorflow_linear_onnx_file = linear_tensorflow_export_model_onnx(tensorflow_linear_model)

tensorflow_linear_surml_file.save(
    path=str(os.path.join(tensorflow_surml_stash_directory, "linear.surml"))
)