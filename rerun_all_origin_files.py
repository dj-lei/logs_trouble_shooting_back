from utils import *
from extract import *

if __name__ == '__main__':
    print('Start rerun all origin files!')
    indices = iterate_files_in_directory(cf['ENV_'+env]['ORIGIN_FILE_STORE_PATH'])
    error_files = []
    for index in indices:
        try:
            name = clean_special_symbols(index,'_')
            print('Start extract:', index)
            fe = FileExtract(cf['ENV_'+env]['ORIGIN_FILE_STORE_PATH'] + index, name)
            if fe.is_extractable:
                fe.extract()
        except:
            error_files.append(index)
    with open(cf['ENV_'+env]['ORIGIN_FILE_STORE_PATH']+'error.log', 'w') as error:
        error.write('\n'.join(error_files))
    print('Finish!')