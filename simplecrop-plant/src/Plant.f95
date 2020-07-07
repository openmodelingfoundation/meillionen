module PlantComponent
    use, intrinsic :: iso_c_binding

    type, public, bind(C) :: PlantInput
        integer(c_int) :: doy, endsim
        real(c_float) :: tmax, tmin, par, swfac1, swfac2
    end type PlantInput

    type, public, bind(C) :: PlantModel
        real(c_float) :: e, fc, lai, nb, n, pt, pg, di, par
        real(c_float) :: rm, dwf, intc, tmax, tmin, p1, sla
        real(c_float) :: pd, emp1, emp2, lfmax, dwc, tmn
        real(c_float) :: dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl
        real(c_float) :: swfac1, swfac2
        integer(c_int) :: doy, endsim, count
    end type PlantModel
contains
    subroutine pm_inititialize_from_file(this) bind(c, name='pm_initialize_from_file')
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

        open (2, file = 'data/plant.inp', status = 'UNKNOWN')
        open (1, file = 'output/plant.out', status = 'REPLACE')

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

    subroutine pm_output_to_file(this) bind(c, name='pm_output_to_file')
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

    subroutine pm_close(this) bind(c, name='pm_close')
        implicit none
        type(PlantModel) :: this
        close(1)
    end subroutine pm_close

    subroutine pm_rate(this) bind(c, name='pm_rate')
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

    subroutine pm_update(this, input) bind(c, name='pm_update')
        use iso_c_binding
        implicit none
        type(PlantModel) :: this
        type(PlantInput) :: input
        this%tmax = input%tmax
        this%tmin = input%tmin
        this%par  = input%par
        this%swfac1 = input%swfac1
        this%swfac2 = input%swfac2
    end subroutine pm_update

    subroutine pm_integ(this) bind(c, name='pm_integ')
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
!subroutine plant(&
!        doy, endsim, tmax, tmin, par, swfac1, swfac2, & !input
!        lai, & !output
!        dyn) bind(c, name='plant')                                            !control
!
!    !-----------------------------------------------------------------------
!    use plantcomponent
!    implicit none
!    save
!
!    integer(c_int) :: doy, endsim
!    real(c_float) :: tmax, tmin, par, swfac1, swfac2, lai
!    character(len=10) dyn
!    type(plantmodel) :: model
!
!    model%doy = doy
!    model%endsim = endsim
!    model%tmax = tmax
!    model%tmin = tmin
!    model%par = par
!    model%swfac1 = swfac1
!    model%swfac2 = swfac2
!    model%lai = lai
!!    real :: e, fc, lai, nb, n, pt, pg, di, par
!!    real :: rm, dwf, int, tmax, tmin, p1, sla
!!    real :: pd, emp1, emp2, lfmax, dwc, tmn
!!    real :: dwr, dw, dn, w, wc, wr, wf, tb, intot, dlai, fl
!!    integer :: doy, endsim, count
!!    character(10) :: dyn
!!
!!    real :: swfac1, swfac2
!
!    !************************************************************************
!    !************************************************************************
!    !     initialization
!    !************************************************************************
!    if (index(dyn, 'INITIAL') /= 0) then
!        !************************************************************************
!        call pm_initialize_from_file(model)
!
!        !************************************************************************
!        !************************************************************************
!        !     rate calculations
!        !************************************************************************
!    elseif (index(dyn, 'RATE') /= 0) then
!        !************************************************************************
!        call pm_rate(model)
!
!        !************************************************************************
!        !************************************************************************
!        !     integration
!        !************************************************************************
!    elseif (index(dyn, 'INTEG') /= 0) then
!        !************************************************************************
!        call pm_integ(model)
!
!        !************************************************************************
!        !************************************************************************
!        !     output
!        !************************************************************************
!    elseif (index(dyn, 'OUTPUT') /= 0) then
!        !************************************************************************
!        call pm_output_to_file(model)
!
!        !************************************************************************
!        !************************************************************************
!        !     close
!        !************************************************************************
!    elseif (index(dyn, 'CLOSE') /= 0) then
!        !************************************************************************
!        call pm_close(model)
!
!        !************************************************************************
!        !************************************************************************
!        !     end of dynamic 'if' construct
!        !************************************************************************
!    endif
!    !************************************************************************
!    return
!end subroutine plant
!!***********************************************************************



!***********************************************************************
!     subroutine lais
!     calculates the canopy leaf area index (lai)
!-----------------------------------------------------------------------
!     input:  fl, di, pd, emp1, emp2, n, nb, swfac1, swfac2, pt, dn
!     output: dlai
!************************************************************************
! pure
subroutine lais(fl, di, pd, emp1, emp2, n, nb, swfac1, swfac2, pt, &
        dn, p1, sla, dlai)

    !-----------------------------------------------------------------------
    implicit none
    save
    real :: pd, emp1, emp2, n, nb, dlai, swfac, a, dn, p1, sla
    real :: swfac1, swfac2, pt, di, fl
    !-----------------------------------------------------------------------

    swfac = min(swfac1, swfac2)
    if (fl == 1.0) then
        a = exp(emp2 * (n - nb))
        dlai = swfac * pd * emp1 * pt * (a / (1 + a)) * dn
    elseif (fl == 2.0) then

        dlai = - pd * di * p1 * sla

    endif
    !-----------------------------------------------------------------------
    return
end subroutine lais
!***********************************************************************



!****************************************************************************
!     subroutine pgs
!     calculates the canopy gross photosysntesis rate (pg)
!*****************************************************************************
! pure
subroutine pgs(swfac1, swfac2, par, pd, pt, lai, pg)

    !-----------------------------------------------------------------------
    implicit none
    save
    real :: par, lai, pg, pt, y1
    real :: swfac1, swfac2, swfac, rowspc, pd

    !-----------------------------------------------------------------------
    !     rowsp = row spacing
    !     y1 = canopy light extinction coefficient

    swfac = min(swfac1, swfac2)
    rowspc = 60.0
    y1 = 1.5 - 0.768 * ((rowspc * 0.01)**2 * pd)**0.1
    pg = pt * swfac * 2.1 * par / pd * (1.0 - exp(-y1 * lai))

    !-----------------------------------------------------------------------
    return
end subroutine pgs
!***********************************************************************



!***********************************************************************
!     subroutine pts
!     calculates the factor that incorporates the effect of temperature
!     on photosynthesis
!************************************************************************
subroutine pts(tmax, tmin, pt)
    !-----------------------------------------------------------------------
    implicit none
    save
    real :: pt, tmax, tmin

    !-----------------------------------------------------------------------
    pt = 1.0 - 0.0025 * ((0.25 * tmin + 0.75 * tmax) - 26.0)**2

    !-----------------------------------------------------------------------
    return
end subroutine pts
!***********************************************************************
!***********************************************************************

!module PlantFFI
!    use, intrinsic :: iso_c_binding
!
!    type, public, bind(C) :: PlantInput
!        integer(c_int) :: doy, endsim
!        real(c_float) :: tmax, tmin, par, swfac1, swfac2, lai
!    end type PlantInput
!contains
!    subroutine plant_initialize(input) bind(c, name = 'plant_initialize')
!        type(PlantInput) :: input
!        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INITIAL   ')
!    end subroutine plant_initialize
!
!    subroutine plant_rate(input) bind(c, name = 'plant_rate')
!        type(PlantInput) :: input
!        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'RATE      ')
!    end subroutine plant_rate
!
!    subroutine plant_integ(input) bind(c, name = 'plant_integ')
!        type(PlantInput) :: input
!        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'INTEG     ')
!    end subroutine plant_integ
!
!    subroutine plant_output(input) bind(c, name = 'plant_output')
!        type(PlantInput) :: input
!        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'OUTPUT    ')
!    end subroutine plant_output
!
!    subroutine plant_close(input) bind(c, name = 'plant_close')
!        type(PlantInput) :: input
!        call plant(input%doy, input%endsim, input%tmax, input%tmin, input%par, input%swfac1, input%swfac2, input%lai, 'CLOSE     ')
!    end subroutine plant_close
!end module PlantFFI