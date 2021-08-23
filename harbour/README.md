# DBFAdapter b.1.0

`.prg` files are Harbour (https://github.com/harbour/core) interface to DBF databases. In order to successfully use the RFKAdapter package it's neccessary for DBFAdapter to be present on the target system. The compilation process for Debian based linux systems is briefly described below.
***
Datoteke `.prg ` su Harbour (https://github.com/harbour/core) interfejs prema DBF bazama podataka. Za uspješno korištenje RFKAdapter paketa neophodno je prethodno osigurati da je izvršna verzija DBFAdapter-a dostupna na ciljnom sistemu. Proces kompilacije za Debian bazirane linux sisteme ukratko je dat ispod.

## Compile

**Required packages**

* ANSI C compiler (gcc)
* GNU Make (3.81, minimum 3.79)
* Harbour source code (2.0.0 or upper)

In order to use RFKAdapter it's first neccessary to compile `.prg` source code into executables and this requires a functioning Harbour compiler, which can be obtained by compiling it as described below:
***
**Neophodni paketi:**

* ANSI C kompajler (gcc)
* GNU Make (3.81, minimum 3.79)
* Harbour izvorni kod (2.0.0 ili iznad)

Za korištenje RFKAdapter neophodno je izvršiti kompilaciju vašeg `.prg` izvornog koda u izvršne datoteke, no za to vam je neophodan Harbour kompajler. Sam Harbour kompajler možete kompajlirati kao u narednom primjeru za Ubuntu/Debian:

### Harbour

```bash
sudo apt install make
sudo apt install git
sudo apt install gcc
sudo apt install binutils
sudo apt install liballegro4-dev
sudo apt install libncurses-dev
sudo apt install libslang2-dev
sudo apt install libx11-dev
sudo apt install libcairo2-dev
sudo apt install libgpm-dev

cd harbour

git clone https://github.com/harbour/core

cd core

make
```

### Docs

Detailed compilation instructions for different operating systems are available at:
***
Detaljna kompilacijska uputstva za različite operacione sisteme dostupna na: 
* https://github.com/harbour/core#linux-deb-based-distros-debian-ubuntu
* http://www.kresin.ru/en/hrbfaq.html#Install5

## .prg 

Compiling the `*.prg` files into executables can be easily achieved by using the `./build` script:
***
Za kompilaciju `*.prg` datoteka u izvršne datoteke koristite `./build` skriptu:

```bash
HB_INS="./core"
$HB_INS/bin/linux/gcc/harbour $1.prg -n -q0 -es2 -gc0 -I$HB_INS/include
gcc -I. -I$HB_INS/include -Wall -c $1.c -o$1.o
gcc -static -Wall -o$1 $1.o -L $HB_INS/lib/linux/gcc/ \
      -Wl,--start-group -lpthread -lbz2 -lhbcomm -lhbmacro -llzf -lgtalleg \
      -lhbmemio -lhbssl -lminilzo -lgtcgi -lhbcpage -lhbmisc -lhbtcpio \
      -lminizip -lgtcrs -lhbcplr -lhbmlzo -lhbtest -lmxml -lgtpca -lhbct \
      -lhbmxml -lhbtinymt -lpng -lgtsln -lhbdebug -lhbmzip -lhbtip -lrddbm \
      -lgtstd -lhbexpat -lhbnetio -lhbtpathy -lrddcdx -lgttrm -lhbextern \
      -lhbnf -lhbuddall -lrddfpt -lgtxwc -lhbformat -lhbunix -lhbsqlit3 \
      -lrddnsx -lhbfoxpro -lhbusrrdd -lrddntx -lhbfship -lhboslib \
      -lhbvm -lrddsql -lhbgt -lhbpcre -lhbvmmt -lsddsqlt3 -lhbamf -lhbgzio \
      -lhbpipeio -lhbxdiff -ltiff -lhbblink -lhbhpdf -lhbpp -lhbxpp -ltinymt \
      -lhbbz2 -lhbhsx -lhbrdd -lhbzebra -lxdiff -lhbbz2io -lhbhttpd -lhbrtl \
      -lhbziparc -lxhb -lhbcairo -lhblang -lhbsix -ljpeg -lhbcomio -lhblzf \
      -lhbsms -llibhpdf -lhbcommon -lpcre -Wl,--end-group -lm -lgpm
```

After the compilation, for the system to be able to find the DBFAdapter executable it's necessary to copy `dbfadapter` into a search `path` directory, e.g. `/usr/local/bin/`. More details below.
***
Nakon kompilacije, a da bi sistem mogao pronaći izvršnu DBFAdapter datoteku neophodno je prekopirati `dbfadapter` u neki od `path` direktorija npr. `/usr/local/bin/`. Više detalja ispod.

* http://www.kresin.ru/en/hrbfaq.html#Compile3

## Harbour Docs

* https://harbour.github.io/doc/
* https://harbour.wiki/
* https://github.com/harbour/core
* http://www.kresin.ru/en/hrbfaq.html
* https://doc.alaska-software.com/content/bc_programming_guide.html
* https://vivaclipper.wordpress.com/
* https://en.wikibooks.org/wiki/Clipper_Tutorial:_a_Guide_to_Open_Source_Clipper(s)
* https://web.archive.org/web/20150623151129/http://www.manmrk.net:80/tutorials/database/dbase/IntrodBASEIIIPlus.htm
* https://www.itlnet.net/programming/program/Reference/c53g01c/nga979b.html
* http://www.elektrosoft.it/tutorials/harbour-how-to/harbour-how-to.asp#standardoutput
* https://dbfree.org/webdocs/1-documentation/a-about_indexes.htm
* http://bb.donnay-software.com/
* https://gnosis.cx/publish/programming/harbour.html
* http://www.fivetechsoft.com/harbour-docs/command.html

## Tools

* https://medium.com/harbour-magazine/visual-studio-code-for-harbour-e148f9c1861a
* https://github.com/APerricone/harbourCodeExtension