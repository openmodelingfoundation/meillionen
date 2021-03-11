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
        real :: pt, tmax, tmin

        !-----------------------------------------------------------------------
        pt = 1.0 - 0.0025 * ((0.25 * tmin + 0.75 * tmax) - 26.0)**2

        !-----------------------------------------------------------------------
        return
    end subroutine pts
    !***********************************************************************
    !***********************************************************************

    subroutine rate(m)
        use iso_c_binding
        implicit none
        type(PlantModel) :: m

        m%tmn = 0.5 * (m%tmax + m%tmin)
        call pts(m%tmax, m%tmin, m%pt)
        call pgs(m%swfac1, m%swfac2, m%par, m%pd, m%pt, m%lai, m%pg)

        if (m%n < m%lfmax) then
            !         vegetative phase
            m%fl = 1.0
            m%e = 1.0
            m%dn = m%rm * m%pt

            call lais(m%fl, m%di, m%pd, m%emp1, m%emp2, m%n, m%nb, m%swfac1, m%swfac2, m%pt, &
                    m%dn, m%p1, m%sla, m%dlai)
            m%dw = m%e * (m%pg) * m%pd
            m%dwc = m%fc * m%dw
            m%dwr = (1 - m%fc) * m%dw
            m%dwf = 0.0

        else
            !         reproductive phase
            m%fl = 2.0

            if (m%tmn >= m%tb .and. m%tmn <= 25) then
                m%di = (m%tmn - m%tb)
            else
                m%di = 0.0
            endif

            m%intc = m%intc + m%di
            m%e = 1.0
            call lais(m%fl, m%di, m%pd, m%emp1, m%emp2, m%n, m%nb, m%swfac1, m%swfac2, m%pt, &
                    m%dn, m%p1, m%sla, m%dlai)
            m%dw = m%e * (m%pg) * m%pd
            m%dwf = m%dw
            m%dwc = 0.0
            m%dwr = 0.0
            m%dn = 0.0
        endif
    end subroutine rate

    subroutine c_rate(m) bind(c, name = 'pm_rate')
        implicit none
        type(PlantModel) :: m
        call rate(m)
    end subroutine c_rate

    subroutine update(m, input)
        type(PlantModel) :: m
        type(PlantInput) :: input
        m%tmax = input%tmax
        m%tmin = input%tmin
        m%par = input%par
        m%swfac1 = input%swfac1
        m%swfac2 = input%swfac2
    end subroutine update

    subroutine c_update(m, input) bind(c, name = 'pm_update')
        implicit none
        type(PlantModel) :: m
        type(PlantInput) :: input
        call update(m, input)
    end subroutine c_update

    subroutine integ(m)
        use iso_c_binding
        implicit none
        type(PlantModel) :: m

        m%lai = m%lai + m%dlai
        m%w = m%w + m%dw
        m%wc = m%wc + m%dwc
        m%wr = m%wr + m%dwr
        m%wf = m%wf + m%dwf

        m%lai = max(m%lai, 0.0)
        m%w = max(m%w, 0.0)
        m%wc = max(m%wc, 0.0)
        m%wr = max(m%wr, 0.0)
        m%wf = max(m%wf, 0.0)

        m%n = m%n + m%dn
        if (m%intc > m%intot) then
            m%endsim = 1
            return
        endif
    end subroutine integ

    subroutine c_integ(m) bind(c, name = 'pm_integ')
        implicit none
        type(PlantModel) :: m
        call integ(m)
    end subroutine c_integ
end module PlantComponent