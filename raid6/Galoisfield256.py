
class Galoisfield256:
    def __init__(self):
        self.irreducible_poly = 0x11d
        self.multi_dict = {}
        self.power_dict = {}
        for i in range(256):
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

                self.multi_dict[str(i)+"*"+str(j)] = res
        
        for i in range(256):
            for j in range(256):
                a = i
                pow = j
                res = 1
                while pow>0:
                    if pow%2 ==1:
                        res = self.multiply(res,a) 
                    a = self.multiply(a,a)
                    pow //= 2
                self.power_dict[str(i)+"**"+str(j)] = res
        
    def add(a,b):
        return a^b
    
    def sub(a,b):
        return a^b
    
    def multiply(self,a,b):
        return self.multi_dict[str(a)+"*"+str(b)]

    def div(self,a,b):
        return self.multiply(a,self.inverse(b))
    
    def power(self,a,pow):
        if pow<0:
            raise ValueError("We do not support negative power. Please use .inverse() instead.")
        elif a == 0:
            return 0
        else:
            return self.power_dict[str(a)+"**"+str(pow)]
        
    def inverse(self,a):
        return self.power(a,254)



