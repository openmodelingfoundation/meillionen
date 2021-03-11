module PlantCLI
    use PlantComponent
    implicit none
contains

    subroutine initialize_from_file(m)
        use iso_c_binding
        implicit none
        type(PlantModel) :: m
        m%endsim = 0

        open (2, file = 'data/plant.inp', status = 'OLD')
        open (1, file = 'output/plant.out', status = 'REPLACE')

        read(2, 10) m%lfmax, m%emp2, m%emp1, m%pd, m%nb, m%rm, m%fc, m%tb &
                , m%intot, m%n, m%lai, m%w, m%wr, m%wc &
                , m%p1, m%sla
        10 format(17(1x, f7.4))
        close(2)

        write(1, 11)
        write(1, 12)
        11 format('Results of plant growth simulation: ')
        12 format(/ &
                /, '                Accum', &
                /, '       Number    Temp                                    Leaf', &
                /, '  Day      of  during   Plant  Canopy    Root   Fruit    Area', &
                /, '   of    Leaf  Reprod  Weight  Weight  Weight  weight   Index', &
                /, ' Year   Nodes    (oC)  (g/m2)  (g/m2)  (g/m2)  (g/m2) (m2/m2)', &
                /, ' ----  ------  ------  ------  ------  ------  ------  ------')

        write(*, 11)
        write(*, 12)

        m%count = 0
    end subroutine initialize_from_file

    subroutine output_to_file(m)
        use iso_c_binding
        implicit none
        type(PlantModel) :: m

        write(1, 20) m%doy, m%n, m%intc, m%w, m%wc, m%wr, m%wf, m%lai
        20 format(i5, 7f8.2)

        if (m%count == 23) then
            m%count = 0
            write(*, 30)
            30 format(2/)
            31 format(/ &
                    /, '                Accum', &
                    /, '       Number    Temp                                    Leaf', &
                    /, '  Day      of  During   Plant  Canopy    Root   Fruit    Area', &
                    /, '   of    Leaf  Reprod  Weight  Weight  Weight  Weight   Index', &
                    /, ' Year   Nodes    (oC)  (g/m2)  (g/m2)  (g/m2)  (g/m2) (m2/m2)', &
                    /, ' ----  ------  ------  ------  ------  ------  ------  ------')
            write(*, 31)
        endif

        m%count = m%count + 1
        write(*, 20) m%doy, m%n, m%intc, m%w, m%wc, m%wr, m%wf, m%lai
    end subroutine output_to_file

    subroutine close_file(m)
        implicit none
        type(PlantModel) :: m
        close(1)
    end subroutine close_file

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
    subroutine plant(&
            doy, endsim, tmax, tmin, par, swfac1, swfac2, & !input
            lai, & !output
            dyn)                                           !control

        !-----------------------------------------------------------------------
        use iso_c_binding
        implicit none
        save

        integer(c_int) :: doy, endsim
        real(c_float) :: tmax, tmin, par, swfac1, swfac2, lai
        character(len = 10) dyn
        type(PlantModel) :: model

        model%doy = doy
        model%endsim = endsim
        model%tmax = tmax
        model%tmin = tmin
        model%par = par
        model%swfac1 = swfac1
        model%swfac2 = swfac2
        model%lai = lai

        !************************************************************************
        !************************************************************************
        !     initialization
        !************************************************************************
        if (index(dyn, 'INITIAL') /= 0) then
            !************************************************************************
            call initialize_from_file(model)

            !************************************************************************
            !************************************************************************
            !     rate calculations
            !************************************************************************
        elseif (index(dyn, 'RATE') /= 0) then
            !************************************************************************
            call rate(model)

            !************************************************************************
            !************************************************************************
            !     integration
            !************************************************************************
        elseif (index(dyn, 'INTEG') /= 0) then
            !************************************************************************
            call integ(model)

            !************************************************************************
            !************************************************************************
            !     output
            !************************************************************************
        elseif (index(dyn, 'OUTPUT') /= 0) then
            !************************************************************************
            call output_to_file(model)

            !************************************************************************
            !************************************************************************
            !     close
            !************************************************************************
        elseif (index(dyn, 'CLOSE') /= 0) then
            !************************************************************************
            call close_file(model)

            !************************************************************************
            !************************************************************************
            !     end of dynamic 'if' construct
            !************************************************************************
        endif
        !************************************************************************
        doy = model%doy
        endsim = model%endsim
        tmax = model%tmax
        tmin = model%tmin
        par = model%par
        swfac1 = model%swfac1
        swfac2 = model%swfac2
        lai = model%lai
    end subroutine plant
    !***********************************************************************
end module PlantCLI