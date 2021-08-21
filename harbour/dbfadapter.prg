#include "dbstruct.ch"

FUNCTION MAIN(command)
    LOCAL i, args := {}
    
    ErrorLevel(0)
    SET DELETED ON
    SET DATE FORMAT "yyyy-mm-dd"
    hb_RegexHas('a', 'a') // *** HAS TO BE HERE IN ORDER TO LOAD THE FUNCTION! ***

    FOR i := 2 TO PCount()
        AAdd(args, hb_Pvalue(i))
    NEXT

    DO CASE
        CASE Upper(command) = "APPEND"
            RETURN APPEND(args)
        CASE Upper(command) = "EXPORT"
            RETURN EXPORT(args)
        CASE Upper(command) = "HEAD"
            RETURN HEAD(args)
        CASE Upper(command) = "UPDATE"
            RETURN UPDATE(args)
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
        ErrorLevel(50)
        OutStd("500 ERROR. LOCKED")
        ErrorLevel()
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
        ErrorLevel(40)
        OutStd("400 ERROR. USAGE e.g: filter ABS_DBPATH NAME.DBF ABSTMPFILEPATH [INDEX01.NTX ...]")
        RETURN ErrorLevel()
    ENDIF
    path := argv[1]
    base := argv[2]
    tmpfile := argv[3]
    Set(_SET_FILECASE, 2)
    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    
    USE (base)
    IF !FLock()
        ErrorLevel(50)
        OutStd("500 ERROR. LOCKED")
        RETURN ErrorLevel()
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            ErrorLevel(50)
            OutStd("500 ERROR. LOCKED")
            RETURN ErrorLevel()
        ENDIF
    NEXT

    Set(_SET_FILECASE, 0)
    where := hb_jsonDecode(memoRead(tmpfile))

    filterString := rfk_CompileFilter(base, where)
    IF .NOT. ValType(filterString) == 'C'
        RETURN ErrorLevel()
    ENDIF
    
    SET FILTER TO &(filterString)
    GOTO TOP

    COPY TO (tmpfile) DELIMITED
    OutStd("200 SUCCESS")

FUNCTION APPEND(argv)
    LOCAL i, path, base, csvfile
    Set(_SET_FILECASE, 2)
    IF Len(argv) < 3
        ErrorLevel(40)
        OutStd("400 ERROR. USAGE e.g: append ABS_DBPATH NAME.DBF ABSCSVFILEPATH [INDEX01.NTX ...]")
        RETURN ErrorLevel()
    ENDIF
    path := argv[1]
    base := argv[2]
    csvfile := argv[3]

    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        ErrorLevel(50)
        OutStd("500 ERROR. LOCKED")
        RETURN ErrorLevel()
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            ErrorLevel(50)
            OutStd("500 ERROR. LOCKED")
            RETURN ErrorLevel()
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    APPEND FROM (csvfile) DELIMITED
    dbSkip(0)
    OutStd("200 SUCCESS")

FUNCTION rfk_CompileFilter(base, where)
    LOCAL filterString := ""
    FOR i := 1 TO Len(where)
        e := where[i]
        eType := ValType(&(base)->&(e['column_name']))
        DO CASE
            CASE eType == 'D'
                e['value'] := "CToD('" + e['value'] + "')"
            CASE eType == 'C'
                e['value'] := "'" + e['value'] + "'"
            CASE eType == 'I'
                e['value'] := Str(e['value'])
            CASE eType == 'N'
                e['value'] := Str(e['value'])
            CASE eType == 'L' .AND. e['value']
                e['value'] := '.T.'
            CASE eType == 'L' .AND. .NOT. (e['value'])
                e['value'] := '.F.'
            OTHERWISE
                ErrorLevel(44)
                OutStd("404 ERROR. INVALID FILTER PARAMETERS VALUE")
                RETURN ErrorLevel()
        ENDCASE
        DO CASE
            CASE e['comparator'] == "lt"
                filterString := filterString + e['column_name'] + " < " + e['value']
            CASE e['comparator'] == "gt"
                filterString := filterString + e['column_name'] + " > " + e['value']
            CASE e['comparator'] == "lte"
                filterString := filterString + e['column_name'] + " <= " + e['value']
            CASE e['comparator'] == "gte"
                filterString := filterString + e['column_name'] + " >= " + e['value']
            CASE e['comparator'] == "eq"
                filterString := filterString + e['column_name'] + " = " + e['value']
            CASE e['comparator'] == "neq"
                filterString := filterString + e['column_name'] + " <> " + e['value']
            CASE e['comparator'] == "si" .AND. eType == 'C'
                filterString := filterString + "At(Lower(" + e['value'] + "), Lower(" + e['column_name'] + ")) > 0"
            CASE e['comparator'] == "s" .AND. eType == 'C'
                filterString := filterString + "At(" + e['value'] + ", " + e['column_name'] + ") > 0"
            CASE e['comparator'] == "x" .AND. eType == 'C'
                filterString := filterString + "hb_RegexHas(" + e['value'] + ", " + e['column_name'] + ")"
            OTHERWISE
                ErrorLevel(44)
                OutStd("404 ERROR. INVALID FILTER PARAMETERS COMPARATOR OR VALUE PAIR")
                RETURN ErrorLevel()
        ENDCASE
        IF i < Len(where)
            filterString := filterString + ' .AND. '
        ENDIF
    NEXT
    RETURN filterString

FUNCTION UPDATE(argv)
    LOCAL i, path, base, jsonFile, request := {}, keys, key, e, filterString := "", insets := {}, finVal, s, f, eType, updatedCount := 0
    Set(_SET_FILECASE, 2)
    IF Len(argv) < 3
        ErrorLevel(40)
        OutStd("400 ERROR. USAGE e.g: update ABS_DBPATH NAME.DBF ABSJSONFILEPATH [INDEX01.NTX ...]")
        RETURN ErrorLevel()
    ENDIF
    path := argv[1]
    base := argv[2]
    jsonFile := argv[3]

    Set(_SET_DEFAULT, hb_DirSepToOS(path))
    USE (base)
    IF !FLock()
        ErrorLevel(50)
        OutStd("500 ERROR. LOCKED")
        RETURN ErrorLevel()
    ENDIF
    FOR i := 4 TO Len(argv)
        SET INDEX TO (argv[i]) ADDITIVE
        IF !FLock()
            ErrorLevel(50)
            OutStd("500 ERROR. LOCKED")
            RETURN ErrorLevel()
        ENDIF
    NEXT
    Set(_SET_FILECASE, 0)
    request := hb_jsonDecode(memoRead(jsonFile))

    filterString := rfk_CompileFilter(base, request['where'])
    IF .NOT. ValType(filterString) == 'C'
        RETURN ErrorLevel()
    ENDIF
    
    keys := hb_HKeys(request['what'])
    SET FILTER TO &(filterString)
    GOTO TOP
    DO WHILE !Eof()
        updatedCount := updatedCount + 1
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
                    s := &(base)->&(SubStr(e, 4, Len(e) - 6))
                    IF ValType(s) == 'D'
                        finVal := AtRepl(e, finVal, DToC(s))
                    ELSE
                        finVal := AtRepl(e, finVal, s)
                    ENDIF
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
    OutStd('UPDATED:' + allTrim(Str(updatedCount)))
    OutStd(hb_eol() + "200 SUCCESS")

RETURN ErrorLevel()