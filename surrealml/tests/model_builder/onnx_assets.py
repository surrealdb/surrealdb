from tests.model_builder.utils import install_package
# install_package("torch==2.1.2")
install_package("onnx==1.16.0")
install_package("scikit-learn==1.4.0") # use sklearn to train the model
install_package("skl2onnx==1.16.0") # use skl2onnx to convert the model to onnx to test the raw onnx loading
import os
import onnx

from surrealml.model_templates.onnx.onnx_linear import train_model as linear_onnx_train_model
from surrealml.model_templates.onnx.onnx_linear import export_model_onnx as linear_onnx_export_model_onnx
from surrealml.model_templates.onnx.onnx_linear import export_model_surml as linear_onnx_export_model_surml

from tests.model_builder.utils import delete_directory, create_directory, MODEL_STASH_DIRECTORY


# create the model stash directory if it does not exist
create_directory(dir_path=MODEL_STASH_DIRECTORY)

# defining directories for onnx
onnx_stash_directory = os.path.join(MODEL_STASH_DIRECTORY, "onnx")
onnx_surml_stash_directory = os.path.join(onnx_stash_directory, "surml")
onnx_onnx_stash_directory = os.path.join(onnx_stash_directory, "onnx")

# delete the directories if they exist
delete_directory(dir_path=onnx_stash_directory)
delete_directory(dir_path=onnx_surml_stash_directory)
delete_directory(dir_path=onnx_onnx_stash_directory)

# create directories for the onnx models
create_directory(dir_path=onnx_stash_directory)
create_directory(dir_path=onnx_surml_stash_directory)
create_directory(dir_path=onnx_onnx_stash_directory)

# train and stash onnx models
onnx_linear_model = linear_onnx_train_model()
onnx_linear_surml_file = linear_onnx_export_model_surml(onnx_linear_model)
onnx_linear_onnx_file = linear_onnx_export_model_onnx(onnx_linear_model)

onnx_linear_surml_file.save(
    path=str(os.path.join(onnx_surml_stash_directory, "linear.surml"))
)
onnx.save(
    onnx_linear_onnx_file,
    os.path.join(onnx_onnx_stash_directory, "linear.onnx")
)
