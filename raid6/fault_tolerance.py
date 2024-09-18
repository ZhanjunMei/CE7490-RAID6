from .Galoisfield256 import Galoisfield256

_mygf = Galoisfield256()

def sum_list(D: list):
    s = 0
    for Di in D:
        s = _mygf.add(s, Di)

    return s

def sum_list_Q(D: list):
    s = D[-1]
    g = 2
    for i in range(len(D)-1):
        s = _mygf.multiply(s, g)
        s = _mygf.add(s, D[-2-i])

    return s

def compute_PQ(D: list) -> list:
    # D is the byte list
    P = sum_list(D[:-2])
    Q = sum_list_Q(D[:-2])

    return [P, Q]

def failure_fix(D: list, pos: list) -> list:
    # D is a row of bytes with P & Q in the end
    total = len(D)
    if len(pos) > 2:
        raise Exception('At most two disk failure at the same time can be fixed')
    if len(pos) == 0:
        return pos
    if len(pos) == 1:
        D_ = D.copy()
        if pos[0] == total-2: # disk with P fails
            P = sum_list(D_[:-2])
            return [P]
        elif pos[0] == total-1: # disk with Q fails
            Q = sum_list_Q(D_[:-2])
            return [Q]
        else: # disk with data fails
            tmp = D_[:-1].copy()
            tmp.pop(pos[0])
            Dx = sum_list(tmp)
            return [Dx]

    if len(pos) == 2:
        if pos[0]>= pos[1]:
            raise Exception('Please make sure pos[0] < pos[1]')
        D_ = D.copy()
        g = 2
        if pos[0]==total-2 and pos[1]==total-1: # disks with P & Q fail
            P = sum_list(D_[:-2])
            Q = sum_list_Q(D_[:-2])
            return [P,Q]

        if pos[1] == total-2: # disks with a data and P fail
            Qx = sum_list_Q(D_[:-2])
            Dx = _mygf.add(D_[-1], Qx)
            Dx = _mygf.multiply(Dx, _mygf.power(g, 255-pos[0]))
            D_[pos[0]] = Dx
            P = sum_list(D_[:-2])
            return [Dx, P]

        if pos[1] == total-1: # disks with a data and Q fail
            Dx = sum_list(D_[:-1])
            D_[pos[0]] = Dx
            Q = sum_list_Q(D_[:-2])
            return [Dx, Q]

        # disks with only data fail
        P = D_[-2]
        Q = D_[-1]
        Pxy = sum_list(D_[:-2])
        Qxy = sum_list_Q(D_[:-2])

        A = _mygf.power(g, pos[1]-pos[0])
        B = _mygf.power(g, 255-pos[0])
        T = _mygf.inverse(_mygf.add(A,1))
        B = _mygf.multiply(B, T)  # A = g^(y-x) *(g^(y-x) + 01)^-1
        A = _mygf.multiply(A, T)  # B = g^(-x) * (g^(y-x) + 01)^-1
        Dx = _mygf.multiply(A, _mygf.add(P, Pxy))
        Dx = _mygf.add(Dx, _mygf.multiply(B, _mygf.add(Q, Qxy))) # Dx = A(P+Pxy)+B(Q+Qxy)
        Dy = _mygf.add(Dx, _mygf.add(P, Pxy))

        return [Dx, Dy]

    return pos


def corruption_check_fix(D: list) -> list:
    # No error return [-1], If error return [pos, correction]
    total = len(D)
    Px = sum_list(D[:-2])
    Qx = sum_list_Q(D[:-2])
    P = D[-2]
    Q = D[-1]
    if P == Px and Q == Qx:
        return [-1, None]
    P_ = _mygf.add(P, Px)
    Q_ = _mygf.add(Q, Qx)
    if P_ == 0: # Q drive corruption
        return [total-1, Qx]
    if Q_ == 0: # P drive corruption
        return [total-2, Px]

    # Data drive corruption
    z = _mygf.log(Q_) - _mygf.log(P_)
    if z < 0: 
        z = z + 255
    if z >= total:
        print(_mygf.log(Q_))
        print(_mygf.log(P_))
    D_ = D.copy()
    D_[z] = 0
    Dz = sum_list(D_[:-1])

    return [z, Dz]


if __name__ == '__main__':
    import random, copy

    # test failure
    # print('--- test failure ---')
    # disk_num = 10
    # steps = 100
    # for i in range(steps):
        # arr = []
        # for _ in range(disk_num):
            # arr.append(random.randint(0, 255))
        # P, Q = compute_PQ(arr)
        # arr[-2] = P
        # arr[-1] = Q
        # failed_arr = copy.deepcopy(arr)
        # if random.random() < 0.5:
            # # single failure
            # idx = random.randint(0, disk_num - 1)
            # failed_arr[idx] = 0
            # recover = failure_fix(failed_arr, [idx])
            # if recover[0] != arr[idx]:
                # print('error')
                # print('arr:', arr)
                # print('idx:', idx)
                # print('recover:', recover)
                # break
        # else:
            # # double failure
            # idx0 = random.randint(0, disk_num - 2)
            # idx1 = random.randint(idx0 + 1, disk_num - 1)
            # failed_arr[idx0] = failed_arr[idx1] = 0
            # recover = failure_fix(failed_arr, [idx0, idx1])
            # if recover[0] != arr[idx0] or recover[1] != arr[idx1]:
                # print('error')
                # print('arr:', arr)
                # print('idx:', [idx0, idx1])
                # print('recover:', recover)
                # break

    # test corruption
    print('--- test corruption ---')
    disk_num = 10
    steps = 100
    for i in range(steps):
        arr = []
        for _ in range(disk_num):
            arr.append(random.randint(0, 255))
        P, Q = compute_PQ(arr)
        arr[-2] = P
        arr[-1] = Q
        corrupted_arr = copy.deepcopy(arr)
        if random.random() < 0.2:
            # no corruption
            res = corruption_check_fix(corrupted_arr)
            if res[0] >= 0:
                print('error: no corruption')
                print(corrupted_arr)
                break
        else:
            idx = random.randint(0, disk_num - 1)
            t = random.randint(0, 255)
            while t == arr[idx]:
                t = random.randint(0, 255)
            corrupted_arr[idx] = t
            res = corruption_check_fix(corrupted_arr)
            if res[0] != idx or res[1] != arr[idx]:
                print('error')
                print('arr:', arr)
                print('corrupted:', [idx, t])
                print('ans:', [idx, arr[idx]])
                print('res:', res)