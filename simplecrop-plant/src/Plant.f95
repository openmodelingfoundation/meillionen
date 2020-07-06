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

module PlantFFI
    use, intrinsic :: iso_c_binding

    type, public, bind(C) :: PlantInput
        integer(c_int) :: doy, endsim
        real(c_float) :: tmax, tmin, par, swfac1, swfac2, lai
    end type PlantInput
contains
    subroutine plant_initialize(input) bind(c, name='plant_initialize')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INITIAL   ')
    end subroutine plant_initialize

    subroutine plant_rate(input) bind(c, name='plant_rate')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'RATE      ')
    end subroutine plant_rate

    subroutine plant_integ(input) bind(c, name='plant_integ')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INTEG     ')
    end subroutine plant_integ

    subroutine plant_output(input) bind(c, name='plant_output')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'OUTPUT    ')
    end subroutine plant_output

    subroutine plant_close(input) bind(c, name='plant_close')
        type(PlantInput) :: input
        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'CLOSE     ')
    end subroutine plant_close
end module PlantFFI