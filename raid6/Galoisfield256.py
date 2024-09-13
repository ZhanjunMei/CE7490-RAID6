
class Galoisfield256:
    def __init__(self):
        self.irreducible_poly = 0x11d
        self.multi_dict = {}
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
        
        


    def add(a,b):
        return a^b
    
    def sub(a,b):
        return a^b
    
    def multiply(self,a,b):
        return self.multi_dict[str(a)+"*"+str(b)]

    def div(self,a,b):
        return self.multiply(a,self.inverse(b))
    
    def power(self,a,pow):
        res = 1
        if pow<0:
            pow = -pow
            while pow>0:
                if pow%2 ==1:
                    res = self.multiply(res,a)
                a = self.multiply(a,a)
                pow //= 2
            return self.inverse(res)

        else:
            while pow>0:
                if pow%2 ==1:
                    res = self.multiply(res,a) 
                a = self.multiply(a,a)
                pow //= 2
            return res
        
    def inverse(self,a):
        return self.power(a,254)


if __name__ == "__main__" :
    GF = Galoisfield256()
    res = GF.power(2,4)
    print(res)
