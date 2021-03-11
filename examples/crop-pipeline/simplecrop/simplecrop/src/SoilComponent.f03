module SoilComponent
    use, intrinsic :: iso_c_binding
    implicit none

    type, public, bind(c) :: SoilInput
        integer(c_int) :: doy
        real(c_float) :: lai, rain, srad, tmax, tmin, swfac1, swfac2
    end type SoilInput

    type, public, bind(c) :: SoilModel
        integer(c_int) :: date, doy
        real(c_float) :: srad, tmax, tmin, rain, swc, inf, irr, rof, esa, epa, drnp
        real(c_float) :: drn, dp, wpp, fcp, stp, wp, fc, st, esp, epp, etp, lai
        real(c_float) :: cn, swfac1, swfac2, potinf
        real(c_float) :: swc_init, train, tirr, tesa, tepa, trof, tdrn
        real(c_float) :: tinf, swc_adj
        real(c_float) :: s, the
    end type SoilModel

    type, public, bind(c) :: IrrigationInput
        integer(c_int) :: date
        real(c_float) :: irr
    end type IrrigationInput
contains
    !************************************************************************
    !     Subroutine DRAINE
    !     Calculates vertical drainage.
    !-----------------------------------------------------------------------
    !     Input:  SWC, FC, DRNp
    !     Output: DRN
    !************************************************************************

    SUBROUTINE DRAINE(SWC, FC, DRNp, DRN)

        !-----------------------------------------------------------------------
        IMPLICIT NONE
        REAL SWC, FC, DRN, DRNp
        !-----------------------------------------------------------------------

        IF (SWC .GT. FC) THEN
            DRN = (SWC - FC) * DRNp
        ELSE
            DRN = 0
        ENDIF

        !-----------------------------------------------------------------------
        RETURN
    END SUBROUTINE DRAINE
    !************************************************************************



    !************************************************************************
    ! *     Subroutine ESaS
    ! *     Calculates the actual daily soil evaporation.
    !-----------------------------------------------------------------------
    ! *     Input:  SWC, WP, FC, ESp
    ! *     Output: ESa
    !************************************************************************

    SUBROUTINE ESaS(ESp, SWC, FC, WP, ESa)

        !-----------------------------------------------------------------------
        IMPLICIT NONE
        REAL a, SWC, WP, FC, ESa, ESp
        !-----------------------------------------------------------------------
        IF (SWC .LT. WP) THEN
            a = 0
        ELSEIF (SWC .GT. FC) THEN
            a = 1
        ELSE
            a = (SWC - WP) / (FC - WP)
        ENDIF

        ESa = ESp * a

        !-----------------------------------------------------------------------
        RETURN
    END SUBROUTINE ESAS
    !************************************************************************



    !************************************************************************
    ! *     Subroutine ETpS
    ! *     Calculates the daily potential evapotranspiration.
    !-----------------------------------------------------------------------
    ! *     Input:  LAI, TMAX, TMIN, SRAD
    ! *     Output: ETp
    !************************************************************************
    ! C
    ! *     Local Variables
    ! *     ALB  =  ALBEDO OF CROP-SOIL SURFACE
    ! *     EEQ  =  EQUILIBRIUM EVAPOTRANSPIRATION (mm)
    ! *     Tmed =  ESTIMATED AVERAGE DAILY TEMPERATURE (C)
    ! *     f    =

    !-----------------------------------------------------------------------
    SUBROUTINE ETpS(SRAD, TMAX, TMIN, LAI, ETp)

        !-----------------------------------------------------------------------
        IMPLICIT NONE
        REAL    ALB, EEQ, f, Tmed, LAI
        REAL TMAX, TMIN, SRAD, ETP

        !-----------------------------------------------------------------------
        ALB = 0.1 * EXP(-0.7 * LAI) + 0.2 * (1 - EXP(-0.7 * LAI))
        Tmed = 0.6 * TMAX + 0.4 * TMIN
        EEQ = SRAD * (4.88E-03 - 4.37E-03 * ALB) * (Tmed + 29)

        IF (TMAX .LT. 5) THEN
            f = 0.01 * EXP(0.18 * (TMAX + 20))
        ELSEIF (TMAX .GT. 35) THEN
            f = 1.1 + 0.05 * (TMAX - 35)
        ELSE
            f = 1.1
        ENDIF

        ETp = f * EEQ
        !-----------------------------------------------------------------------
        RETURN
    END SUBROUTINE ETPS
    !************************************************************************



    !************************************************************************
    ! *     Subroutine RUNOFF
    ! *     Calculates the daily runoff.
    !************************************************************************
    ! *     Input:  POTINF, CN
    ! *     Output: ROF
    ! !-----------------------------------------------------------------------
    ! *     Local Variables
    ! *     CN = CURVE NUMBER SCS EQUATION
    ! *     S  = WATERSHED STORAGE SCS EQUATION (MM)

    !-----------------------------------------------------------------------

    subroutine runoff_rate(potinf, rof, s)
        implicit none
        real :: potinf, rof, s

        IF (POTINF .GT. 0.2 * S)  THEN
            ROF = ((POTINF - 0.2 * S)**2) / (POTINF + 0.8 * S)
        ELSE
            ROF = 0
        ENDIF
    end subroutine runoff_rate

    !************************************************************************
    ! *     Sub-subroutine STRESS calculates soil water stresses.
    ! *     Today's stresses will be applied to tomorrow's rate calcs.
    !-----------------------------------------------------------------------
    ! *     Input:  SWC, DP, FC, ST, WP
    ! *     Output: SWFAC1, SWFAC2
    !************************************************************************

    subroutine stress_integ(SWC, DP, FC, ST, WP, SWFAC1, SWFAC2, the)
        implicit none
        real :: swc, dp, fc, st, wp, swfac1, swfac2, the
        real, parameter :: STRESS_DEPTH = 250
        real :: wtable, dwt
        IF (SWC .LT. WP) THEN
            SWFAC1 = 0.0
        ELSEIF (SWC .GT. THE) THEN
            SWFAC1 = 1.0
        ELSE
            SWFAC1 = (SWC - WP) / (THE - WP)
            SWFAC1 = MAX(MIN(SWFAC1, 1.0), 0.0)
        ENDIF

        !-----------------------------------------------------------------------
        !     Excess water stress factor - SWFAC2
        !-----------------------------------------------------------------------
        IF (SWC .LE. FC) THEN
            WTABLE = 0.0
            DWT = DP * 10.              !DP in cm, DWT in mm
            SWFAC2 = 1.0
        ELSE
            !FC water is distributed evenly throughout soil profile.  Any
            !  water in excess of FC creates a free water surface
            !WTABLE - thickness of water table (mm)
            !DWT - depth to water table from surface (mm)
            WTABLE = (SWC - FC) / (ST - FC) * DP * 10.
            DWT = DP * 10. - WTABLE

            IF (DWT .GE. STRESS_DEPTH) THEN
                SWFAC2 = 1.0
            ELSE
                SWFAC2 = DWT / STRESS_DEPTH
            ENDIF
            SWFAC2 = MAX(MIN(SWFAC2, 1.0), 0.0)
        ENDIF
    end subroutine stress_integ

    !************************************************************************
    ! *     Subroutine WBAL
    ! *     Seasonal water balance
    !-----------------------------------------------------------------------
    !     Input:  SWC, SWC_INIT, TDRN, TEPA,
    !                 TESA, TIRR, TRAIN, TROF
    !     Output: None
    !************************************************************************

    SUBROUTINE WBAL(SWC_INIT, SWC, TDRN, TEPA, &
            TESA, TIRR, TRAIN, TROF, SWC_ADJ, TINF)

        !-----------------------------------------------------------------------
        IMPLICIT NONE
        INTEGER, PARAMETER :: LSWC = 21
        REAL SWC, SWC_INIT
        REAL TDRN, TEPA, TESA, TIRR, TRAIN, TROF
        REAL WATBAL, SWC_ADJ, TINF
        REAL CHECK
        !-----------------------------------------------------------------------
        OPEN (LSWC, FILE = 'output/wbal.out', STATUS = 'REPLACE')

        WATBAL = (SWC_INIT - SWC) + (TRAIN + TIRR) - &
                !      0.0   =(Change in storage)+  (Inflows)    -
                (TESA + TEPA + TROF + TDRN)
        !                         (Outflows)

        WRITE(*, 100)   SWC_INIT, SWC, TRAIN, TIRR, TESA, TEPA, TROF, TDRN
        WRITE(LSWC, 100)SWC_INIT, SWC, TRAIN, TIRR, TESA, TEPA, TROF, TDRN
        100 FORMAT(//, 'SEASONAL SOIL WATER BALANCE', //, &
                'Initial soil water content (mm):', F10.3, /, &
                'Final soil water content (mm):  ', F10.3, /, &
                'Total rainfall depth (mm):      ', F10.3, /, &
                'Total irrigation depth (mm):    ', F10.3, /, &
                'Total soil evaporation (mm):    ', F10.3, /, &
                'Total plant transpiration (mm): ', F10.3, /, &
                'Total surface runoff (mm):      ', F10.3, /, &
                'Total vertical drainage (mm):   ', F10.3, /)

        IF (SWC_ADJ .NE. 0.0) THEN
            WRITE(*, 110) SWC_ADJ
            WRITE(LSWC, 110) SWC_ADJ
            110   FORMAT('Added water for SWC<0 (mm):     ', E10.3, /)
        ENDIF

        WRITE(*, 200) WATBAL
        WRITE(LSWC, 200) WATBAL
        200 FORMAT('Water Balance (mm):             ', F10.3, //)

        CHECK = TRAIN + TIRR - TROF
        IF ((CHECK - TINF) .GT. 0.0005) THEN
            WRITE(*, 300) CHECK, TINF, (CHECK - TINF)
            WRITE(LSWC, 300) CHECK, TINF, (CHECK - TINF)
            300   FORMAT(/, 'Error: TRAIN + TIRR - TROF = ', F10.4, /, &
                    'Total infiltration =         ', F10.4, /, &
                    'Difference =                 ', F10.4)
        ENDIF

        CLOSE (LSWC)

        !-----------------------------------------------------------------------
        RETURN
    END SUBROUTINE WBAL


    !************************************************************************
    !************************************************************************

    subroutine rate(m)
        implicit none
        type(SoilModel) :: m

        m%TIRR = m%TIRR + m%IRR
        m%POTINF = m%RAIN + m%IRR
        m%TRAIN = m%TRAIN + m%RAIN
        CALL DRAINE(m%SWC, m%FC, m%DRNp, m%DRN)

        IF (m%POTINF .GT. 0.0) THEN
            CALL runoff_rate(m%POTINF, m%ROF, m%s)
            m%INF = m%POTINF - m%ROF
        ELSE
            m%ROF = 0.0
            m%INF = 0.0
        ENDIF

        !     Potential evapotranspiration (ETp), soil evaporation (ESp) and
        !       plant transpiration (EPp)
        CALL ETpS(m%SRAD, m%TMAX, m%TMIN, m%LAI, m%ETp)
        m%ESp = m%ETp * EXP(-0.7 * m%LAI)
        m%EPp = m%ETp * (1 - EXP(-0.7 * m%LAI))

        !     Actual soil evaporation (ESa), plant transpiration (EPa)
        CALL ESaS(m%ESp, m%SWC, m%FC, m%WP, m%ESa)
        m%EPa = m%EPp * MIN(m%SWFAC1, m%SWFAC2)
    end subroutine rate

    subroutine c_rate(m) bind(c, name = 'soil_rate')
        implicit none
        type(SoilModel) :: m
        call rate(m)
    end subroutine c_rate

    subroutine integ(m)
        type(SoilModel) :: m
        m%SWC = m%SWC + (m%INF - m%ESa - m%EPa - m%DRN)

        IF (m%SWC .GT. m%ST) THEN
            m%ROF = m%ROF + (m%SWC - m%ST)
            m%SWC = m%ST
        ENDIF

        IF (m%SWC .LT. 0.0) THEN
            m%SWC_ADJ = m%SWC_ADJ - m%SWC
            m%SWC = 0.0
        ENDIF

        m%TINF = m%TINF + m%INF
        m%TESA = m%TESA + m%ESA
        m%TEPA = m%TEPA + m%EPA
        m%TDRN = m%TDRN + m%DRN
        m%TROF = m%TROF + m%ROF

        call stress_integ(m%SWC, m%DP, m%FC, m%ST, m%WP, m%SWFAC1, m%SWFAC2, m%the)
    end subroutine integ

    subroutine c_integ(m) bind(c, name = 'soil_integ')
        implicit none
        type(SoilModel) :: m
        call integ(m)
    end subroutine c_integ
end module SoilComponent