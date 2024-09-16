import Galoisfield256 as gf


def sum_list(D: list):
    mygf = gf
    s = 0
    for Di in D:
        s = mygf.add(s, Di)
        
    return s
    
def sum_list_Q(D: list):
    mygf = gf
    s = D[-1]
    g = 2
    for i range(len(D)-1):
        s = mygf.multiply(s, g)
        s = mygf.add(s, D[-2-i])
        
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
            tmp = D_[:-2].copy()
            tmp.pop(pos[0])
            Dx = sum_list(tmp)
            return [Dx]
        
    if len(pos) == 2:
        if pos[0 ]>= pos[1]:
            raise Exception('Please make sure pos[0] < pos[1]')
        mygf = gf
        D_ = D.copy()
        g = gf(2)
        if pos[0]==total-2 and pos[1]==total-1: # disks with P & Q fail
            P = sum_list(D_[:-2])
            Q = sum_list_Q(D_[:-2])
            return [P,Q]
            
        if pos[1] == total-2: # disks with a data and P fail
            Qx = sum_list_Q(D_[:-2])
            Dx = mygf.add(Q, Qx)
            Dx = mygf.multiply(Dx, mygf.power(g, 255-pos[0]))
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

        A = mygf.power(g, pos[1]-pos[0]) 
        B = mygf.power(g, 255-pos[0])    
        T = mygf.inverse(mygf.add(A,1))
        B = mygf.multiply(B, T)  # A = g^(y-x) *(g^(y-x) + 01)^-1
        A = mygf.multiply(A, T)  # B = g^(-x) * (g^(y-x) + 01)^-1
        Dx = mygf.multiply(A, mygf.add(P, Pxy))
        Dx = mygf.add(Dx, mygf.multiply(B, mygf.add(Q, Qxy))) # Dx = A(P+Pxy)+B(Q+Qxy)
        Dy = mygf.add(Dx, mygf.add(P, Pxy))
        
        return [Dx, Dy]
    
    return pos


def corruption_check_fix(D: list) -> list:
    # No error return [-1], If error return [pos, correction]
    mygf = gf
    total = len(D)
    Px = sum_list(D[:-2])
    Qx = sum_list_Q(D[:-2])
    P = D[-2]
    Q = D[:-1]
    if P == Px and Q == Qx:
        return [-1]
    P_ = mygf.add(P, Px)
    Q_ = mygf.add(Q, Qx)
    if P_ == 0: # Q drive corruption
        return [total-1, Qx]
    if Q_ == 0: # P drive corruption
        return [total-2, Px]
    
    # Data drive corruption
    z = mygf.sub(mygf.log(Q_), mygf.log(P_))
    D_ = D.copy()
    D_[z] = 0
    Dz = sum_list(D_[:-1])

    return [z, Dz]
