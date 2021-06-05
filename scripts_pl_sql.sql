  
--drop table company_tic;
--sqlldr userid = chapid/oracle control=control_company.ctl
--sqlldr userid = usuario/password control = archivo de control para subir con extension ctl
  
CREATE TABLE company_tic(
  COMPANY varchar2(40),
  CONTROL_DATE date,
  OPEN_VALUE NUMBER(15,9),
  HIGH_VALUE NUMBER(15,9),
  LOW_VALUE NUMBER(15,9),
  CLOSE_VALUE NUMBER(15,9),
  ADJ_CLOSE NUMBER(15,9),
  VOLUME_VALUE NUMBER(15,2)
 );
 
 
--drop table report_company_tic;
CREATE TABLE report_company_tic(
    COMPANY varchar2(40),
    Q1_VALUE NUMBER(15,9),
    QUARTER_RANGE_Q1 VARCHAR2(40),
    VARIATION_Q1 NUMBER(15,9),
    DATE_BEST_SALE_Q1 varchar2(40),
    Q2_VALUE NUMBER(15,9),
    QUARTER_RANGE_Q2 VARCHAR2(40),
    VARIATION_Q2 NUMBER(15,9),
    DATE_BEST_SALE_Q2 varchar2(40),
    Q3_VALUE NUMBER(15,9),
    QUARTER_RANGE_Q3 VARCHAR2(40),
    VARIATION_Q3 NUMBER(15,9),
    DATE_BEST_SALE_Q3 varchar2(40),
    Q4_VALUE NUMBER(15,9),
    QUARTER_RANGE_Q4 VARCHAR2(40),
    VARIATION_Q4 NUMBER(15,9),
    DATE_BEST_SALE_Q4 varchar2(40),
    BEST_Q  VARCHAR2(40)
 );

 CREATE TABLE TEMP_BESTQ(
 COMPANY VARCHAR2(40),
 Q_VALUE VARCHAR2(5),
 VALUE_BEST_Q NUMBER(17,6)
 );
 
 --DROP TABLE TEMP_BESTQ;
***************************************************************************************************************************
***************************************************************************************************************************
/*****
ESTE PROCEDIMIENTO LLENA UNA TABLA TEMPORAL CON LOS DATOS DE LOS Q1,Q2,Q3 YQ4 PARA CALCULAR EL MEJOR Q DEL AÑO
****/
create or replace PROCEDURE calcular_bestq(p_anio NUMBER,p_company VARCHAR2)
IS
v_avg_volumeq1 NUMBER;
v_avg_adjq1 NUMBER;
v_avg_volumeq2 NUMBER;
v_avg_adjq2 NUMBER;
v_avg_volumeq3 NUMBER;
v_avg_adjq3 NUMBER;
v_avg_volumeq4 NUMBER;
v_avg_adjq4 NUMBER;
v_totalq NUMBER(16,6);
BEGIN
    SELECT TRUNC(AVG(volume_value),5) AS prom_volume_value,TRUNC(AVG(adj_close),5) AS prom_adj_close
    INTO v_avg_volumeq1,v_avg_adjq1
    FROM company_tic
    WHERE control_date >= '01/01/'||p_anio AND control_date <= '31/03/'||p_anio
    AND company = p_company;
    v_totalq:=TRUNC((v_avg_volumeq1*v_avg_adjq1),5);
    INSERT INTO temp_bestq VALUES(p_company,'Q1',v_totalq);
    SELECT TRUNC(AVG(volume_value),5) AS prom_volume_value,TRUNC(AVG(adj_close),5) AS prom_adj_close
    INTO v_avg_volumeq2,v_avg_adjq2
    FROM company_tic
    WHERE control_date >= '01/04/'||p_anio AND control_date <= '30/06/'||p_anio
    AND company = p_company;
    v_totalq:=v_avg_volumeq2*v_avg_adjq2;
    INSERT INTO temp_bestq VALUES(p_company,'Q2',v_totalq);
    SELECT TRUNC(AVG(volume_value),5) AS prom_volume_value,TRUNC(AVG(adj_close),5) AS prom_adj_close
    INTO v_avg_volumeq3,v_avg_adjq3
    FROM company_tic
    WHERE control_date >= '01/07/'||p_anio AND control_date <= '30/09/'||p_anio
    AND company = p_company;
    v_totalq:=v_avg_volumeq3*v_avg_adjq3;
    INSERT INTO temp_bestq VALUES(p_company,'Q3',v_totalq);
    SELECT TRUNC(AVG(volume_value),5) AS prom_volume_value,TRUNC(AVG(adj_close),5) AS prom_adj_close
    INTO v_avg_volumeq4,v_avg_adjq4
    FROM company_tic
    WHERE control_date >= '01/10/'||p_anio AND control_date <= '31/12/'||p_anio
    AND company = p_company;
    v_totalq:=v_avg_volumeq4*v_avg_adjq4;
    INSERT INTO temp_bestq VALUES(p_company,'Q4',v_totalq);
END calcular_bestq;

/*****
ESTE PROCEDIMIENTO LLENA EL REPORTE FINAL
****/
create or replace NONEDITIONABLE PROCEDURE finalreport(p_anio VARCHAR2)
IS
BEGIN
DECLARE
    CURSOR company_cursor IS
        SELECT company 
        FROM company_tic 
        GROUP BY company;
    v_promedioq1 NUMBER;
    v_rangoq1 varchar2(45);
    v_variationq1 NUMBER;
    v_maxdateq1 VARCHAR2(45);
    v_promedioq2 NUMBER;
    v_rangoq2 varchar2(45);
    v_variationq2 NUMBER;
    v_maxdateq2 VARCHAR2(45);
    v_promedioq3 NUMBER;
    v_rangoq3 varchar2(45);
    v_variationq3 NUMBER;
    v_maxdateq3 VARCHAR2(45);
    v_promedioq4 NUMBER;
    v_rangoq4 varchar2(45);
    v_variationq4 NUMBER;
    v_maxdateq4 VARCHAR2(45);
    v_bestq VARCHAR2(45);
BEGIN
        FOR c_company IN company_cursor LOOP
            v_promedioq1:=calc_prom(c_company.company,'01/01/'||p_anio,'31/03/'||p_anio);
            v_rangoq1:=calc_qrange(c_company.company,'01/01/'||p_anio,'31/03/'||p_anio);
            v_variationq1:=calc_variation(c_company.company,'01/01/'||p_anio,'31/03/'||p_anio);
            v_maxdateq1:=calc_maxdate(c_company.company,'01/01/'||p_anio,'31/03/'||p_anio);
            v_promedioq2:=calc_prom(c_company.company,'01/04/'||p_anio,'30/06/'||p_anio);
            v_rangoq2:=calc_qrange(c_company.company,'01/04/'||p_anio,'30/06/'||p_anio);
            v_variationq2:=calc_variation(c_company.company,'01/04/'||p_anio,'30/06/'||p_anio);
            v_maxdateq2:=calc_maxdate(c_company.company,'01/04/'||p_anio,'30/06/'||p_anio);
            v_promedioq3:=calc_prom(c_company.company,'01/07/'||p_anio,'30/09/'||p_anio);
            v_rangoq3:=calc_qrange(c_company.company,'01/07/'||p_anio,'30/09/'||p_anio);
            v_variationq3:=calc_variation(c_company.company,'01/07/'||p_anio,'30/09/'||p_anio);
            v_maxdateq3:=calc_maxdate(c_company.company,'01/07/'||p_anio,'30/09/'||p_anio);
            v_promedioq4:=calc_prom(c_company.company,'01/10/'||p_anio,'31/12/'||p_anio);
            v_rangoq4:=calc_qrange(c_company.company,'01/10/'||p_anio,'31/12/'||p_anio);
            v_variationq4:=calc_variation(c_company.company,'01/10/'||p_anio,'31/12/'||p_anio);
            v_maxdateq4:=calc_maxdate(c_company.company,'01/10/'||p_anio,'31/12/'||p_anio);
            calcular_bestq(p_anio,c_company.company);
            v_bestq:=get_bestq(c_company.company);
            INSERT INTO report_company_tic VALUES(c_company.company,
                                                v_promedioq1,
                                                v_rangoq1,
                                                v_variationq1,
                                                v_maxdateq1,
                                                v_promedioq2,
                                                v_rangoq2,v_variationq2,
                                                v_maxdateq2,
                                                v_promedioq3,
                                                v_rangoq3,
                                                v_variationq3,
                                                v_maxdateq3,
                                                v_promedioq4,
                                                v_rangoq4,
                                                v_variationq4,
                                                v_maxdateq4,
                                                v_bestq);
        END LOOP;
END;
END finalreport;

***************************************************************************************************************************
***************************************************************************************************************************
/********
Eliminar los registros de las tablas temp_bestq y reportcompany
*********/
create or replace PROCEDURE vaciar_tablas
IS
BEGIN
    DELETE FROM report_company_tic;
    DELETE FROM TEMP_BESTQ;
END vaciar_tablas;


/*********
BLOQUE ANONIMO QUE CORRE EL PROYECTO
**********/
DECLARE       
    v_anio_reporte number:=&anio;
    TYPE company_table_type IS TABLE OF
        company_tic.volume_value%type
    INDEX BY BINARY_INTEGER;
    company_table company_table_type;
BEGIN
    vaciar_tablas;
    SELECT MAX(volume_value)
    INTO company_table(0)
    FROM company_tic
    WHERE TO_CHAR(control_date,'YYYY') = v_anio_reporte;
    IF company_table.COUNT > 0 THEN
        finalreport(v_anio_reporte);
    END IF;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      DBMS_OUTPUT.PUT_LINE('** El anio solicitado no cuenta con registros **');
END;


--*** se altera el nls para poder asignar las fecha que se solicitan en el reporte
--ALTER session set NLS_DATE_FORMAT='DD/MM/YYYY';
--select * from nls_session_parameters;
--select TO_DATE('04/01/2020','DD/MM/YYYY') from dual;
--select TO_CHAR(TO_DATE('04/01/2020','DD/MM/YYYY'),'MON DD, YYYY') from dual;

***************************************************************************************************************************
***************************************************************************************************************************
/*********
Esta función se encarga de obtener el promedio
de la suma entre el HIGH y LOW por Q.
**********/
create or replace FUNCTION calc_prom(p_company VARCHAR2,fechaini DATE,fechafin DATE)
RETURN NUMBER
IS
v_high_value NUMBER;
v_low_value NUMBER;
v_avg_highlow NUMBER;
BEGIN
    SELECT SUM(high_value) AS high_value,SUM(low_value) AS low_value
    INTO v_high_value,v_low_value
    FROM company_tic
    WHERE control_date >= fechaini AND control_date <= fechafin
    AND company = p_company;
    v_avg_highlow:=(v_high_value+v_low_value)/2;
    RETURN v_avg_highlow;
END calc_prom;

/*********
Esta función se encarga de calcular el rango entre
el valor mas alto de la columna HIGH y el valor
mas bajo de la columna LOW por cada Q.
**********/
create or replace FUNCTION calc_qrange(p_company VARCHAR2,fechaini DATE, fechafin DATE)
RETURN VARCHAR2
IS
v_maxhigh_value NUMBER;
v_minlow_value NUMBER;
v_range VARCHAR2(45);
BEGIN
    SELECT MAX(high_value) AS maxhigh_value,MIN(low_value) AS minlow_value
    INTO v_maxhigh_value,v_minlow_value
    FROM company_tic
    WHERE control_date >= fechaini AND control_date <= fechafin
    AND company = p_company;
    v_range:= TO_CHAR(v_minlow_value)||' - '||TO_CHAR(v_maxhigh_value);
    RETURN v_range;
END calc_qrange;

/*********
Esta función calcula el valor promedio de apertura
menos el valor promedio de cierre por cada Q.
**********/
create or replace FUNCTION calc_variation(p_company VARCHAR2,fechaini DATE, fechafin DATE)
RETURN NUMBER
IS
v_avg_open NUMBER;
v_avg_close NUMBER;
v_variation NUMBER;
BEGIN
    SELECT TRUNC(AVG(open_value),5) AS prom_open ,TRUNC(AVG(close_value),5) AS prom_close
    INTO v_avg_open,v_avg_close
    FROM company_tic
    WHERE control_date >= fechaini AND control_date <= fechafin
    AND company = p_company;
    v_variation:=v_avg_open-v_avg_close;
    RETURN v_variation;
END calc_variation;

/*********
Esta función se encarga de obtener la fecha en
donde se presento el valor mas alto de la
columna HIGH por cada Q.
**********/
create or replace FUNCTION calc_maxdate(p_company VARCHAR2,fechaini DATE, fechafin DATE)
RETURN VARCHAR2
IS
v_date_max_high VARCHAR2(45);
BEGIN
    SELECT MAX(CONTROL_DATE)
    INTO v_date_max_high
    FROM company_tic
    WHERE control_date >= fechaini AND control_date <= fechafin
    AND company = p_company
    AND high_value IN (select max(high_value) 
                        from company_tic
                        WHERE control_date >= fechaini
                        AND control_date <= fechafin
                        AND company = p_company);
    v_date_max_high:= TO_CHAR(TO_DATE(v_date_max_high,'DD/MM/YYYY'),'MON DD, YYYY');
    RETURN v_date_max_high;
END calc_maxdate;

create or replace PROCEDURE vaciar_tablas
IS
BEGIN
    DELETE FROM report_company_tic;
    DELETE FROM TEMP_BESTQ;
END vaciar_tablas;

/*****
Funcion para calcular el mejor Q del anio
****/
create or replace FUNCTION get_bestq(p_company VARCHAR2)
RETURN VARCHAR2
IS
v_maxbest_q NUMBER;
v_maxq VARCHAR2(5);
v_bestq VARCHAR2(45);
BEGIN
    SELECT value_best_q,q_value
    INTO v_maxbest_q,v_maxq
    FROM temp_bestq
    WHERE company = p_company
    AND value_best_q = (SELECT MAX(value_best_q)
                        FROM temp_bestq
                        WHERE company = p_company);
    v_bestq:= TO_CHAR(v_maxq||'='||v_maxbest_q);
    RETURN v_bestq;
END get_bestq;


