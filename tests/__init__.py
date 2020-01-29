import logging
debug = logging.getLogger('test').debug

def display_filespecs(filespecs, piece_size):
    filecount = len(filespecs)
    header = [' ' * (((4*filecount) + (2*filecount-1)) + 2)]
    for i in range(6):
        header.append(str(i) + ' '*(piece_size-1))
    debug(''.join(header))
    line = (', '.join(f'{fn}:{fs:2d}' for fn,fs in filespecs),
            ' - ',
            ''.join(fn*fs for fn,fs in filespecs))
    debug(''.join(line))
