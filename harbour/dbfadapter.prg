#include "dbstruct.ch"

FUNCTION MAIN(command)
    LOCAL i, args := {}
    
    SET DELETED ON
    SET DATE FORMAT "yyyy-mm-dd"

    FOR i := 2 TO PCount()
        AAdd(args, hb_Pvalue(i))
    NEXT

    DO CASE
        CASE Upper(command) = "APPEND"
            APPEND(args)
        CASE Upper(command) = "EXPORT"
            EXPORT(args)
        CASE Upper(command) = "HEAD"
            HEAD(args)
        CASE Upper(command) = "UPDATE"
            UPDATE(args)
    ENDCASE

FUNCTION HEAD(argv)
    LOCAL field, struct, path, base, tmpfile, output := {}
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: filter ABS_DBPATH NAME.DBF ABSTMPFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    tmpfile := argv[3]
    Set(_SET_FILECASE, 2)
    Set(_SET_DEFAULT, hb_DirSepToOS(path))

    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF

    Set(_SET_FILECASE, 0)
    struct := dbStruct()
    FOR EACH field IN struct
        AAdd(output, '<' + allTrim(hb_ValToStr(field[DBS_NAME])) + ", " + allTrim(hb_ValToStr(field[DBS_TYPE])) + ", " + allTrim(hb_ValToStr(field[DBS_LEN])) + ", " + allTrim(hb_ValToStr(field[DBS_DEC])) + '>')
    NEXT
    hb_memoWrit(tmpfile, hb_jsonEncode(output))
    OutStd("200 SUCCESS")

FUNCTION EXPORT(argv)
    LOCAL i, path, base, tmpfile
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: filter ABS_DBPATH NAME.DBF ABSTMPFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    tmpfile := argv[3]
    Set(_SET_FILECASE, 2)
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    COPY TO (tmpfile) DELIMITED
    OutStd("200 SUCCESS")

FUNCTION APPEND(argv)
    LOCAL i, path, base, csvfile
    Set(_SET_FILECASE, 2)
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: append ABS_DBPATH NAME.DBF ABSCSVFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    csvfile := argv[3]

    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    APPEND FROM (csvfile) DELIMITED
    dbSkip(0)
    OutStd("200 SUCCESS")

FUNCTION UPDATE(argv)
    LOCAL i, path, base, jsonFile, request := {}, keys, key, e, filterString := "", insets := {}, finVal, s, f
    Set(_SET_FILECASE, 2)
    IF Len(argv) < 3
        OutStd("400 ERROR. USAGE e.g: update ABS_DBPATH NAME.DBF ABSJSONFILEPATH [INDEX01.NTX ...]")
        RETURN 1
    ENDIF
    path := argv[1]
    base := argv[2]
    jsonFile := argv[3]

    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        OutStd("500 ERROR. LOCKED")
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            OutStd("500 ERROR. LOCKED")
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    request := hb_jsonDecode(memoRead(jsonFile))

    // @TODO: solve escaping, escape C in 'where', preserve types
    // @TODO: test in python
    FOR i := 1 TO Len(request['where'])
        e := request['where'][i]
        DO CASE
            CASE e['comparator'] == "lt"
                filterString := filterString + e['column_name'] + " < '" + e['value'] + "'"
            CASE e['comparator'] == "gt"
                filterString := filterString + e['column_name'] + " > '" + e['value'] + "'"
            CASE e['comparator'] == "lte"
                filterString := filterString + e['column_name'] + " <= '" + e['value'] + "'"
            CASE e['comparator'] == "gte"
                filterString := filterString + e['column_name'] + " >= '" + e['value'] + "'"
            CASE e['comparator'] == "eq"
                filterString := filterString + e['column_name'] + " = '" + e['value'] + "'"
            CASE e['comparator'] == "neq"
                filterString := filterString + e['column_name'] + " <> '" + e['value'] + "'"
            CASE e['comparator'] == "si"
                filterString := filterString + "At('" + Lower(e['value']) + "', Lower(" + e['column_name'] + ")) > 0"
            CASE e['comparator'] == "s"
                filterString := filterString + "At('" + e['value'] + "', " + e['column_name'] + ") > 0"
            CASE e['comparator'] == "x"
                filterString := filterString + "hb_RegexHas('" + e['value'] + "', " + e['column_name'] + ")"
        ENDCASE
        IF i < Len(request['where'])
            filterString := filterString + ' .AND. '
        ENDIF
    NEXT
    
    keys := hb_HKeys(request['what'])
    hb_RegexHas('a', 'a') // *** HAS TO BE HERE IN ORDER TO LOAD THE FUNCTION! ***
    SET FILTER TO &(filterString)
    GOTO TOP
    DO WHILE !Eof()
        FOR EACH key IN keys
            insets := {}
            i := 1
            DO WHILE ValType(request['what'][key]) == 'C' .AND. i + 6 < Len(request['what'][key])
                s := hb_At('__{', SubStr(request['what'][key], i))
                f := hb_At('}__', SubStr(request['what'][key], s))
                IF s > 0 .AND. f > s
                    e := SubStr(SubStr(request['what'][key], i), s, f + 2)
                    AAdd(insets, e)
                ENDIF
                i := i + f + 3
            ENDDO
            finVal := request['what'][key]
            FOR EACH e IN insets
                IF Len(hb_Regex('__{\w+?}__', e)) > 0
                    finVal := AtRepl(e, finVal, &(base)->&(SubStr(e, 4, Len(e) - 6)))
                ENDIF
            NEXT
            IF ValType(&(base)->&(key)) == 'D'
                &(base)->&(key) := CToD(finVal)
            ELSE
                &(base)->&(key) := finVal
            ENDIF
        NEXT
        dbSkip()
    ENDDO
    OutStd("200 SUCCESS")

RETURN 0