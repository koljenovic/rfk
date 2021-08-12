# Harbour API 0.2.1

Datoteke `.prg ` su Harbour (https://github.com/harbour/core) interfejs prema DBF bazama i trenutno samo nadopunjavaju nedostajuće funkcionalnosti dbf modula, u bućnosti kompletan modul treba zamijeniti Harbour interfejsom.

## Kompilacija

**Neophodni paketi:**

* ANSI C kompajler (gcc)
* GNU Make (3.81, minimum 3.79)
* Harbour izvorni kod (2.0.0 or upper)

Za korištenje Harbour funkcionalnosti neophodno je izvršiti kompilaciju vašeg `.prg` izvornog koda u izvršne datoteke, za to vam je potreban Harbour kompajler koji možete kompajlirati kao u narednom primjeru za Ubuntu/Debian:

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

Detaljna kompilacijska uputstva za različite operacione sisteme dostupna na: 
* https://github.com/harbour/core#linux-deb-based-distros-debian-ubuntu
* http://www.kresin.ru/en/hrbfaq.html#Install5

### .prg kompilacija

Za kompilaciju `*.prg` datoteka u izvršne datoteke koristite `./build` skriptu:

```bash
HB_INS="./core"
$HB_INS/bin/linux/gcc/harbour $1.prg -n -q0 -es2 -gc0 -I$HB_INS/include
gcc -I. -I$HB_INS/include -Wall -c $1.c -o$1.o
gcc -Wall -o$1 $1.o -L $HB_INS/lib/linux/gcc/ \
      -Wl,--start-group -lhbvm -lhbrtl -lhblang -lhbrdd \
      -lhbmacro -lhbpp -lhbcommon -lrddntx -lrddcdx -lrddfpt -lhbsix \
      -lhbct -lgttrm -lhbcpage -Wl,--end-group -lm -lgpm
```

Više detalja na:
* http://www.kresin.ru/en/hrbfaq.html#Compile3

## Harbour dokumentacija

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

## Alati

* https://medium.com/harbour-magazine/visual-studio-code-for-harbour-e148f9c1861a
* https://github.com/APerricone/harbourCodeExtension