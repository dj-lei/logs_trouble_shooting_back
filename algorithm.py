# import torch
# import numpy as np
# from tslearn.metrics import dtw_path
# from tslearn.metrics import lcss_path
# from transformers import BertTokenizer, BertModel
# from sklearn.preprocessing import LabelEncoder
# from sklearn.preprocessing import OneHotEncoder


# device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
# tokenizer = BertTokenizer.from_pretrained('google/bert_uncased_L-2_H-128_A-2')
# model = BertModel.from_pretrained("google/bert_uncased_L-2_H-128_A-2")
# model = model.to(device)
# print("\n Algorithm use ", device)


############################################ Text ecoder ################################################
def pretrained_model_encode_msg(object1, object2):
    with torch.no_grad():
        object1_inputs = tokenizer(list(object1.msg.values), padding=True, truncation=True, return_tensors="pt").to(device)
        object1_outputs = model(**object1_inputs)

        object2_inputs = tokenizer(list(object2.msg.values), padding=True, truncation=True, return_tensors="pt").to(device)
        object2_outputs = model(**object2_inputs)
    return object1_outputs, object2_outputs


def onehot_encode_string(str1, str2):
    data = []
    data.extend(str1)
    data.extend(str2)
    values = np.array(data)

    # integer encode
    label_encoder = LabelEncoder().fit(values)
    integer_encoded = label_encoder.transform(values)

    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    onehot_encoder = OneHotEncoder(sparse=False).fit(integer_encoded)

    encoder1 = onehot_encoder.transform(label_encoder.transform(str1).reshape(len(str1), 1))
    encoder2 = onehot_encoder.transform(label_encoder.transform(str2).reshape(len(str2), 1))
    # inverted = label_encoder.inverse_transform([argmax(onehot_encoded[0, :])])
    # print(inverted)
    return encoder1, encoder2


############################################ Algorithm ################################################
def cal_lcss_path_and_score(s_y1, s_y2):
    path, score = lcss_path(s_y1, s_y2)
    return path, score


def cal_dtw_path_and_score(s_y1, s_y2):
    path, score = dtw_path(s_y1, s_y2)
    return path, score