import os
import json
import xgboost as xgb
import numpy as np

def model_fn(model_dir):
    # Carga el modelo JSON
    bst = xgb.Booster()
    bst.load_model(os.path.join(model_dir, "man_classifier.json")) 
    return bst

def input_fn(request_body, content_type):
    if content_type == 'text/csv':
        # Convierte CSV string a lista de floats
        data = [float(x) for x in request_body.split(',')]
        # XGBoost requiere DMatrix
        return xgb.DMatrix(np.array(data).reshape(1, -1))
    raise ValueError(f"Content type {content_type} not supported")

def predict_fn(input_object, model):
    prediction = model.predict(input_object)
    return prediction.tolist()