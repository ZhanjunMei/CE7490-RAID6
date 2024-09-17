
class Galoisfield256:
    def __init__(self):
        self.irreducible_poly = 0x11d
        self.multi_dict = []
        self.power_dict = []
        for i in range(256):
            temp = []
            for j in range(256):
                res = 0
                a=i
                b=j
                while(b > 0):
                    if b & 1:
                        res = res ^ a
                    a = a<<1
                    if a & 0x100:
                        a ^= self.irreducible_poly
                    b = b>>1
                temp.append(res)
            self.multi_dict.append(temp)
        
        for i in range(256):
            temp = []
            for j in range(256):
                a = i
                pow = j
                res = 1
                while pow>0:
                    if pow%2 ==1:
                        res = self.multi_dict[a][res]
                    a = self.multi_dict[a][a]
                    pow //= 2
                temp.append(res)
            self.power_dict.append(temp)
        
    def add(a,b):
        return a^b
    
    def sub(a,b):
        return a^b
    
    def multiply(self,a,b):
        return self.multi_dict[a][b]

    def div(self,a,b):
        return self.multiply[a][self.inverse(b)]
    
    def power(self,a,pow):
        if pow<0:
            raise ValueError("We do not support negative power. Please use .inverse() instead.")
        elif a == 0:
            return 0
        else:
            return self.power_dict[a][pow]
        
    def inverse(self,a):
        if a == 0:
            raise ValueError("0 does not have inverse!")
        return self.power(a,254)

    def log(self,a):

        if a == 0:
            raise ValueError("the definition field of log does not cover 0!")
        for i in range(256):
            if self.power_dict[2][i] == a:
                return i


