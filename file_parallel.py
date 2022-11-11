import ray

def clean_special_symbols(text, symbol):
    for ch in ['/','*','{','}','[',']','(',')','#','+','-','!','=',':',',','"','\'','>','<','@','`','$','%','^','&','|']:
        if ch in text:
            text = text.replace(ch,symbol)
    return text


@ray.remote
class FileParallel(object):
    def __init__(self):
        pass
        
    def extract(self, lines, r_min, r_max):
        return_dict= {}
        num_range = range(r_min, r_max)
        for index, line in enumerate(lines):
            for word in set(clean_special_symbols(line,' ').split(' ')):
                if len(word) > 0:
                    if (not word[0].isdigit()) & (word not in ['\n', ' ']):
                        if (word not in return_dict):
                            return_dict[word] = [num_range[index]]
                        else:
                            return_dict[word].append(num_range[index])
        return return_dict
    
