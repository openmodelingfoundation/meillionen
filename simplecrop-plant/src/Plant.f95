!***********************************************************************
!***********************************************************************
!     Subroutine PLANT
!     This subroutine simulates the growth of the plant using pre-determined
!     conditions.Hourly values of temperature and photosyntetically active
!     radiation come from WEATHER subroutine and daily values of availability
!     of water in the soil come from SW subroutine. This subroutine supplies
!     the SW subroutine with daily values of leaf area index (LAI).
!****************************************************************************

!                  LIST OF VARIABLES
!     di    = daily accumulated temperature above tb (degree days)
!     dLAI  = daily increase in leaf area index (m2/m2/d)
!     dN    = incremental leaf number
!     DOY   = day of the year
!     DYN   = dynamic control variable
!     dw    = incremental total plant dry matter weight (g m-2)
!     dwc   = incremental canopy dry matter weight (g m-2)
!     dwf   = incremental fruit dry matter weight (g m-2)
!     dwr   = incremental root dry matter weight (g m-2)
!     E     = conversion efficiency of CH2O to plant tissue (g g-1)
!     EMP1  = empirical coef. for expoilinear eq.
!     EMP2  = empirical coef. for expoilinear eq.
!     endsim= code signifying physiological maturity (end of simulation)
!     Fc    = fraction of total crop growth partitioned to canopy
!     FL    = code for development phase (1=vegetative phase,
!                 2=reproductive phase)
!     int   = accumulated temperature after reproductive phase starts (c)
!     INTOT = duration of reproductive stage (degree days)
!     LAI   = canopy leaf area index (m2 m-2)
!     Lfmax = maximum number of leaves
!     N     = leaf number
!     nb    = empirical coef. for expoilinear eq.
!     p1    = dry matter of leaves removed per plant per unit development after
!              maximum number of leaves is reached (g)
!     PD    = plant density m-2
!     Pg    = canopy gross photosynthesis rate (g plant-1 day-1)
!     PT    = photosynthesis reduction factor for temp.
!     rm    = maximum rate of leaf appearearance (day-1)
!     sla   = specific leaf area (m2 g-1)
!     SRAD  = Daily solar radiation (MJ m-2)
!     SWFAC1= soil water deficit stress factor
!     SWFAC2= soil water excess stress factor
!     tb    = base temperature above which reproductive growth occurs (c)
!     TMAX  = Daily maximum temperature (c)
!     TMIN  = Daily manimum temperature (c)
!     TMN   = Daily mean temperature (c)
!     W     = total plant dry matter weight (g m-2)
!     Wc    = canopy dry matter weight (g m-2)
!     Wf    = fruit dry matter weight (g m-2)
!     Wr    = root dry matter weight (g m-2)


!***********************************************************************
SUBROUTINE PLANT(&
        DOY, endsim, TMAX, TMIN, PAR, SWFAC1, SWFAC2, & !Input
        LAI, & !Output
        DYN)                                            !Control

    !-----------------------------------------------------------------------
    IMPLICIT NONE
    SAVE

    REAL :: E, Fc, Lai, nb, N, PT, Pg, di, PAR
    REAL :: rm, dwf, int, TMAX, TMIN, p1, sla
    REAL :: PD, EMP1, EMP2, Lfmax, dwc, TMN
    REAL :: dwr, dw, dn, w, wc, wr, wf, tb, intot, dLAI, FL
    INTEGER :: DOY, endsim, COUNT
    CHARACTER(10) :: DYN

    REAL :: SWFAC1, SWFAC2

    !************************************************************************
    !************************************************************************
    !     INITIALIZATION
    !************************************************************************
    IF (INDEX(DYN, 'INITIAL') /= 0) THEN
        !************************************************************************
        endsim = 0

        OPEN (2, FILE = 'data/Plant.inp', STATUS = 'UNKNOWN')
        OPEN (1, FILE = 'output/plant.out', STATUS = 'REPLACE')

        READ(2, 10) Lfmax, EMP2, EMP1, PD, nb, rm, fc, tb, intot, n, lai, w, wr, wc &
                , p1, sla
        10 FORMAT(17(1X, F7.4))
        CLOSE(2)

        WRITE(1, 11)
        WRITE(1, 12)
        11 FORMAT('Results of plant growth simulation: ')
        12 FORMAT(/ &
                /, '                Accum', &
                /, '       Number    Temp                                    Leaf', &
                /, '  Day      of  during   Plant  Canopy    Root   Fruit    Area', &
                /, '   of    Leaf  Reprod  Weight  Weight  Weight  weight   Index', &
                /, ' Year   Nodes    (oC)  (g/m2)  (g/m2)  (g/m2)  (g/m2) (m2/m2)', &
                /, ' ----  ------  ------  ------  ------  ------  ------  ------')

        WRITE(*, 11)
        WRITE(*, 12)

        COUNT = 0

        !************************************************************************
        !************************************************************************
        !     RATE CALCULATIONS
        !************************************************************************
    ELSEIF (INDEX(DYN, 'RATE') /= 0) THEN
        !************************************************************************
        TMN = 0.5 * (TMAX + TMIN)
        CALL PTS(TMAX, TMIN, PT)
        CALL PGS(SWFAC1, SWFAC2, PAR, PD, PT, Lai, Pg)

        IF (N < Lfmax) THEN
            !         Vegetative phase
            FL = 1.0
            E = 1.0
            dN = rm * PT

            CALL LAIS(FL, di, PD, EMP1, EMP2, N, nb, SWFAC1, SWFAC2, PT, &
                    dN, p1, sla, dLAI)
            dw = E * (Pg) * PD
            dwc = Fc * dw
            dwr = (1 - Fc) * dw
            dwf = 0.0

        ELSE
            !         Reproductive phase
            FL = 2.0

            IF (TMN >= tb .AND. TMN <= 25) THEN
                di = (TMN - tb)
            ELSE
                di = 0.0
            ENDIF

            int = int + di
            E = 1.0
            CALL LAIS(FL, di, PD, EMP1, EMP2, N, nb, SWFAC1, SWFAC2, PT, &
                    dN, p1, sla, dLAI)
            dw = E * (Pg) * PD
            dwf = dw
            dwc = 0.0
            dwr = 0.0
            dn = 0.0
        ENDIF

        !************************************************************************
        !************************************************************************
        !     INTEGRATION
        !************************************************************************
    ELSEIF (INDEX(DYN, 'INTEG') /= 0) THEN
        !************************************************************************
        LAI = LAI + dLAI
        w = w + dw
        wc = wc + dwc
        wr = wr + dwr
        wf = wf + dwf

        LAI = MAX(LAI, 0.0)
        W = MAX(W, 0.0)
        WC = MAX(WC, 0.0)
        WR = MAX(WR, 0.0)
        WF = MAX(WF, 0.0)

        N = N + dN
        IF (int > INTOT) THEN
            endsim = 1
            WRITE(1, 14) doy
            14 FORMAT(/, '  The crop matured on day ', I3, '.')
            RETURN
        ENDIF

        !************************************************************************
        !************************************************************************
        !     OUTPUT
        !************************************************************************
    ELSEIF (INDEX(DYN, 'OUTPUT') /= 0) THEN
        !************************************************************************
        WRITE(1, 20) DOY, n, int, w, wc, wr, wf, lai
        20 FORMAT(I5, 7F8.2)

        IF (COUNT == 23) THEN
            COUNT = 0
            WRITE(*, 30)
            30 FORMAT(2/)
            WRITE(*, 12)
        ENDIF

        COUNT = COUNT + 1
        WRITE(*, 20) DOY, n, int, w, wc, wr, wf, lai

        !************************************************************************
        !************************************************************************
        !     CLOSE
        !************************************************************************
    ELSEIF (INDEX(DYN, 'CLOSE') /= 0) THEN
        !************************************************************************
        CLOSE(1)

        !************************************************************************
        !************************************************************************
        !     End of dynamic 'IF' construct
        !************************************************************************
    ENDIF
    !************************************************************************
    RETURN
END SUBROUTINE PLANT
!***********************************************************************



!***********************************************************************
!     Subroutine LAIS
!     Calculates the canopy leaf area index (LAI)
!-----------------------------------------------------------------------
!     Input:  FL, di, PD, EMP1, EMP2, N, nb, SWFAC1, SWFAC2, PT, dN
!     Output: dLAI
!************************************************************************
! pure
SUBROUTINE LAIS(FL, di, PD, EMP1, EMP2, N, nb, SWFAC1, SWFAC2, PT, &
        dN, p1, sla, dLAI)

    !-----------------------------------------------------------------------
    IMPLICIT NONE
    SAVE
    REAL :: PD, EMP1, EMP2, N, nb, dLAI, SWFAC, a, dN, p1, sla
    REAL :: SWFAC1, SWFAC2, PT, di, FL
    !-----------------------------------------------------------------------

    SWFAC = MIN(SWFAC1, SWFAC2)
    IF (FL == 1.0) THEN
        a = exp(EMP2 * (N - nb))
        dLAI = SWFAC * PD * EMP1 * PT * (a / (1 + a)) * dN
    ELSEIF (FL == 2.0) THEN

        dLAI = - PD * di * p1 * sla

    ENDIF
    !-----------------------------------------------------------------------
    RETURN
END SUBROUTINE LAIS
!***********************************************************************



!****************************************************************************
!     Subroutine PGS
!     Calculates the canopy gross photosysntesis rate (PG)
!*****************************************************************************
! pure
SUBROUTINE PGS(SWFAC1, SWFAC2, PAR, PD, PT, Lai, Pg)

    !-----------------------------------------------------------------------
    IMPLICIT NONE
    SAVE
    REAL :: PAR, Lai, Pg, PT, Y1
    REAL :: SWFAC1, SWFAC2, SWFAC, ROWSPC, PD

    !-----------------------------------------------------------------------
    !     ROWSP = row spacing
    !     Y1 = canopy light extinction coefficient

    SWFAC = MIN(SWFAC1, SWFAC2)
    ROWSPC = 60.0
    Y1 = 1.5 - 0.768 * ((ROWSPC * 0.01)**2 * PD)**0.1
    Pg = PT * SWFAC * 2.1 * PAR / PD * (1.0 - EXP(-Y1 * LAI))

    !-----------------------------------------------------------------------
    RETURN
END SUBROUTINE PGS
!***********************************************************************



!***********************************************************************
!     Subroutine PTS
!     Calculates the factor that incorporates the effect of temperature
!     on photosynthesis
!************************************************************************
SUBROUTINE PTS(TMAX, TMIN, PT)
    !-----------------------------------------------------------------------
    IMPLICIT NONE
    SAVE
    REAL :: PT, TMAX, TMIN

    !-----------------------------------------------------------------------
    PT = 1.0 - 0.0025 * ((0.25 * TMIN + 0.75 * TMAX) - 26.0)**2

    !-----------------------------------------------------------------------
    RETURN
END SUBROUTINE PTS
!***********************************************************************
!***********************************************************************

module PlantComponent
    use iso_c_binding

    type, public, bind(C) :: PlantModel
        real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par
        real(c_float) :: rm, dwf, intc, tmax, tmin, p1, sla
        real(c_float) :: pd, emp1, emp2, lfmax, dwc, tmn
        real(c_float) :: dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl
        real(c_float) :: swfac1, swfac2
        integer(c_int) :: doy, endsim, count
    end type PlantModel
contains
    subroutine pm_inititialize_from_file(this)
        use iso_c_binding
        implicit none
        type(PlantModel) :: this
        real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par, &
                rm, dwf, int, tmax, tmin, p1, sla, &
                pd, emp1, emp2, lfmax, dwc, tmn, &
                dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl, &
                swfac1, swfac2
        integer(c_int) :: doy, endsim, count
        e = this%e
        fc = this%fc
        lai = this%lai
        nb = this%nb
        n = this%n
        pt = this%pt
        pg = this%pg
        di = this%di
        par = this%par

        rm = this%rm
        dwf = this%dwf
        int = this%intc
        tmax = this%tmax
        tmin = this%tmin
        p1 = this%p1
        sla = this%sla

        pd = this%pd
        emp1 = this%emp1
        emp2 = this%emp2
        lfmax = this%lfmax
        dwc = this%dwc
        tmn = this%tmn

        dwr = this%dwr
        dw = this%dw
        dn = this%dn
        w = this%w
        wc = this%wc
        wr = this%wr
        wf = this%wf
        tb = this%tb
        intot = this%intot
        dlai = this%dlai
        fl = this%fl

        swfac1 = this%swfac1
        swfac2 = this%swfac2

        doy = this%doy
        endsim = this%endsim
        count = this%count

        endsim = 0

        open (2, file = 'data/plant.inp', status = 'unknown')
        open (1, file = 'output/plant.out', status = 'replace')

        read(2, 10) lfmax, emp2, emp1, pd, nb, rm, fc, tb, intot, n, lai, w, wr, wc &
                , p1, sla
        10 format(17(1x, f7.4))
        close(2)

        write(1, 11)
        write(1, 12)
        11 format('results of plant growth simulation: ')
        12 format(/ &
                /, '                accum', &
                /, '       number    temp                                    leaf', &
                /, '  day      of  during   plant  canopy    root   fruit    area', &
                /, '   of    leaf  reprod  weight  weight  weight  weight   index', &
                /, ' year   nodes    (oc)  (g/m2)  (g/m2)  (g/m2)  (g/m2) (m2/m2)', &
                /, ' ----  ------  ------  ------  ------  ------  ------  ------')

        write(*, 11)
        write(*, 12)

        count = 0
    end subroutine pm_inititialize_from_file

    subroutine pm_output_to_file(this)
        implicit none
        type(PlantModel) :: this
                real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par, &
                rm, dwf, int, tmax, tmin, p1, sla, &
                pd, emp1, emp2, lfmax, dwc, tmn, &
                dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl, &
                swfac1, swfac2
        integer(c_int) :: doy, endsim, count
        e = this%e
        fc = this%fc
        lai = this%lai
        nb = this%nb
        n = this%n
        pt = this%pt
        pg = this%pg
        di = this%di
        par = this%par

        rm = this%rm
        dwf = this%dwf
        int = this%intc
        tmax = this%tmax
        tmin = this%tmin
        p1 = this%p1
        sla = this%sla

        pd = this%pd
        emp1 = this%emp1
        emp2 = this%emp2
        lfmax = this%lfmax
        dwc = this%dwc
        tmn = this%tmn

        dwr = this%dwr
        dw = this%dw
        dn = this%dn
        w = this%w
        wc = this%wc
        wr = this%wr
        wf = this%wf
        tb = this%tb
        intot = this%intot
        dlai = this%dlai
        fl = this%fl

        swfac1 = this%swfac1
        swfac2 = this%swfac2

        doy = this%doy
        endsim = this%endsim
        count = this%count

        write(1, 20) doy, n, int, w, wc, wr, wf, lai
        20 format(i5, 7f8.2)

        if (count == 23) then
            count = 0
            write(*, 30)
            30 format(2/)
            31 format(/ &
                /, '                accum', &
                /, '       number    temp                                    leaf', &
                /, '  day      of  during   plant  canopy    root   fruit    area', &
                /, '   of    leaf  reprod  weight  weight  weight  weight   index', &
                /, ' year   nodes    (oc)  (g/m2)  (g/m2)  (g/m2)  (g/m2) (m2/m2)', &
                /, ' ----  ------  ------  ------  ------  ------  ------  ------')
            write(*, 31)
        endif

        count = count + 1
        write(*, 20) doy, n, int, w, wc, wr, wf, lai
    end subroutine pm_output_to_file

    subroutine pm_close(this)
        implicit none
        type(PlantModel) :: this
        close(1)
    end subroutine pm_close

    subroutine pm_rate(this)
        use iso_c_binding
        implicit none
        type(PlantModel) :: this
        real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par, &
                rm, dwf, int, tmax, tmin, p1, sla, &
                pd, emp1, emp2, lfmax, dwc, tmn, &
                dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl, &
                swfac1, swfac2
        integer(c_int) :: doy, endsim, count
        e = this%e
        fc = this%fc
        lai = this%lai
        nb = this%nb
        n = this%n
        pt = this%pt
        pg = this%pg
        di = this%di
        par = this%par

        rm = this%rm
        dwf = this%dwf
        int = this%intc
        tmax = this%tmax
        tmin = this%tmin
        p1 = this%p1
        sla = this%sla

        pd = this%pd
        emp1 = this%emp1
        emp2 = this%emp2
        lfmax = this%lfmax
        dwc = this%dwc
        tmn = this%tmn

        dwr = this%dwr
        dw = this%dw
        dn = this%dn
        w = this%w
        wc = this%wc
        wr = this%wr
        wf = this%wf
        tb = this%tb
        intot = this%intot
        dlai = this%dlai
        fl = this%fl

        swfac1 = this%swfac1
        swfac2 = this%swfac2

        doy = this%doy
        endsim = this%endsim
        count = this%count

        tmn = 0.5 * (tmax + tmin)
        call pts(tmax, tmin, pt)
        call pgs(swfac1, swfac2, par, pd, pt, lai, pg)

        if (n < lfmax) then
            !         vegetative phase
            fl = 1.0
            e = 1.0
            dn = rm * pt

            call lais(fl, di, pd, emp1, emp2, n, nb, swfac1, swfac2, pt, &
                    dn, p1, sla, dlai)
            dw = e * (pg) * pd
            dwc = fc * dw
            dwr = (1 - fc) * dw
            dwf = 0.0

        else
            !         reproductive phase
            fl = 2.0

            if (tmn >= tb .and. tmn <= 25) then
                di = (tmn - tb)
            else
                di = 0.0
            endif

            int = int + di
            e = 1.0
            call lais(fl, di, pd, emp1, emp2, n, nb, swfac1, swfac2, pt, &
                    dn, p1, sla, dlai)
            dw = e * (pg) * pd
            dwf = dw
            dwc = 0.0
            dwr = 0.0
            dn = 0.0
        endif
    end subroutine pm_rate

    subroutine pm_integ(this)
        use iso_c_binding
        implicit none
        type(PlantModel) :: this
        real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par, &
                rm, dwf, int, tmax, tmin, p1, sla, &
                pd, emp1, emp2, lfmax, dwc, tmn, &
                dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl, &
                swfac1, swfac2
        integer(c_int) :: doy, endsim, count
        e = this%e
        fc = this%fc
        lai = this%lai
        nb = this%nb
        n = this%n
        pt = this%pt
        pg = this%pg
        di = this%di
        par = this%par

        rm = this%rm
        dwf = this%dwf
        int = this%intc
        tmax = this%tmax
        tmin = this%tmin
        p1 = this%p1
        sla = this%sla

        pd = this%pd
        emp1 = this%emp1
        emp2 = this%emp2
        lfmax = this%lfmax
        dwc = this%dwc
        tmn = this%tmn

        dwr = this%dwr
        dw = this%dw
        dn = this%dn
        w = this%w
        wc = this%wc
        wr = this%wr
        wf = this%wf
        tb = this%tb
        intot = this%intot
        dlai = this%dlai
        fl = this%fl

        swfac1 = this%swfac1
        swfac2 = this%swfac2

        doy = this%doy
        endsim = this%endsim
        count = this%count

        lai = lai + dlai
        w = w + dw
        wc = wc + dwc
        wr = wr + dwr
        wf = wf + dwf

        lai = max(lai, 0.0)
        w = max(w, 0.0)
        wc = max(wc, 0.0)
        wr = max(wr, 0.0)
        wf = max(wf, 0.0)

        n = n + dn
        if (int > intot) then
            endsim = 1
            return
        endif
    end subroutine pm_integ
end module PlantComponent

module PlantFFI
    use, intrinsic :: iso_c_binding

    type, public, bind(C) :: PlantInput
        integer(c_int) :: doy, endsim
        real(c_float) :: tmax, tmin, par, swfac1, swfac2, lai
    end type PlantInput
contains
    subroutine plant_initialize(input) bind(c, name = 'plant_initialize')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INITIAL   ')
    end subroutine plant_initialize

    subroutine plant_rate(input) bind(c, name = 'plant_rate')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'RATE      ')
    end subroutine plant_rate

    subroutine plant_integ(input) bind(c, name = 'plant_integ')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INTEG     ')
    end subroutine plant_integ

    subroutine plant_output(input) bind(c, name = 'plant_output')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'OUTPUT    ')
    end subroutine plant_output

    subroutine plant_close(input) bind(c, name = 'plant_close')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'CLOSE     ')
    end subroutine plant_close
end module PlantFFI