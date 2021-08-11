function MAIN(path)
LOCAL GetList := {}
LOCAL mT
Set( _SET_FILECASE, 2 )
? path
SET DEFAULT TO /home/koljenovic/.rfk/data/ 
* SET DEFAULT TO path
DIR
USE uliz INDEX uliz01, uliz02, uliz03
REINDEX
return nil
