import  os
import logging
from    util import const

logger = logging.getLogger(const.PROJECT_NAME)

def comp_bin_file(file1,file2,len_difference,point_difference):

    cnt = 0
    read_idx = 0

    fsize1 = os.path.getsize(file1)
    fsize2 = os.path.getsize(file2)

    per_comp = abs((fsize1 / fsize2) - 1) * 100
    if (per_comp > len_difference): return False

    f1 = open(file1, 'rb')
    f2 = open(file2, 'rb')

    try:

        while( read_idx < fsize1 and read_idx < fsize2 ):
            fb1 = f1.read(1)
            fb2 = f2.read(1)

            if( fb1 != fb2): cnt = cnt + 1

            if cnt > point_difference:
                break

            read_idx = read_idx + 1

    except IOError:

        f2.close()
        f1.close()
        return False

    f2.close()
    f1.close()

    if cnt > point_difference: return False

    return True


def remove_similar_pictrue_from_path(path_name,len_difference,point_difference):

    logger.info("compare pictue file.")
    file_list = []

    for files in os.listdir(path_name):
        file_name = os.path.join(path_name, files)
        if os.path.isfile(file_name):
            file_list.append(file_name)

    split_file_cnt = len(file_list)
    access_file_cnf = split_file_cnt

    curidx=0
    nextidx=1
    while nextidx < len(file_list):

        if comp_bin_file(file_list[curidx],file_list[nextidx],len_difference,point_difference):
            os.remove(file_list[nextidx])
            access_file_cnf = access_file_cnf - 1
            nextidx = nextidx + 1
        else:
            curidx = nextidx
            nextidx = nextidx + 1

    return split_file_cnt,access_file_cnf

