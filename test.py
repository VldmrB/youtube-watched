"""

"""
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d


def test_two():
    def refine(fine: int, kind: str):
        x_refined = np.linspace(x.min(), x.max(), fine)
        func = interp1d(x, y, kind=kind)
        y_refined = func(x_refined)
        # plt.plot(x_refined, y_refined)
        # plt.show()
        return x_refined, y_refined

    x = np.array([*range(1, 13)])
    y = np.array([np.random.randint(0, 12) for i in range(12)])
    # x_refined = np.linspace(x.min(), x.max(), 18)
    # func = interp1d(x, y, kind='cubic')
    # y_refined = func(x_refined)
    # plt.plot(x_refined, y_refined)
    # plt.show()

    # plt.figure(1)
    fidelity_list = [12, 24, 48, 96, 800, 6000]
    # for fidelity in range(len(fidelity_list)):
    #     plt.subplot(611 + fidelity)
    #     refine(fidelity_list[fidelity], 'cubic')
    # plt.show()
    count = 0
    f, axarr = plt.subplots(len(fidelity_list), 1, sharex=True, sharey=True)
    for fidelity in fidelity_list:
        if fidelity == 12:
            kwargs = {'color': 'orange'}
        else:
            kwargs = {}
        elems = refine(fidelity, 'cubic')
        # axarr[count].scatter(elems[0], elems[1], **kwargs)
        axarr[count].set_title('Breakpoints: ' + str(fidelity))
        axarr[count].plot(elems[0], elems[1], **kwargs)

        # axarr[count].legend()
        count += 1
    f.subplots_adjust(hspace=0.55)
    plt.show()


test_two()

