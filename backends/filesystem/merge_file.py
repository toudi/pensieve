from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .file_wrapper import TimeStreamFile


def mergeInPlace(
    array: "TimeStreamFile",
    num_new_items: int,
    num_all_items: int,
    swap: Callable[[int, int], None],
    progress: Callable[[int], None],
    visualize: bool = False,
):
    print(f"num_new_items: {num_new_items}; num_all_items: {num_all_items}")
    # print(array)
    if num_all_items == 1:
        return

    arrayEnd = num_all_items - 1
    rightMinimumIndex = num_all_items - num_new_items

    lookups = 0
    swaps = 0

    rightMinimum = array[rightMinimumIndex]
    index = 0

    lastSwapIndex = -1

    print(f"firstNewPoint={rightMinimumIndex}; rightMinimum: {rightMinimum}")

    # check if the file needs to be sorted in the first place:
    if array[rightMinimumIndex] >= array[rightMinimumIndex - 1]:
        # nope, the data is already sorted.
        print(
            f"array[{rightMinimumIndex}] ({rightMinimum}) >= array[{rightMinimumIndex-1}] ({array[rightMinimumIndex - 1]})"
        )
        return

    while index < arrayEnd:
        # print(f"current minimum: {array[index]}")
        while array[index] < rightMinimum:
            index += 1
            if index > arrayEnd:
                break
            lookups += 1

        # lastSwapIndex = index

        if index <= arrayEnd and array[index] > array[rightMinimumIndex]:
            swaps += 1
            # print(f"swap {index} with {rightMinimumIndex}")
            if visualize:
                swaps_array = ["  " for _ in range(arrayEnd + 1)]
                swaps_array[index] = "v "
                swaps_array[rightMinimumIndex] = "v "
                print("[" + " ".join(swaps_array) + "]")
                print(array)
            swap(index, rightMinimumIndex)
            if visualize:
                print(array)
            if lastSwapIndex == -1:
                lastSwapIndex = index
            # lastSwapIndex = index
            # print(f"set lastSwapIndex to {lastSwapIndex}")

        index += 1

        if index >= rightMinimumIndex:
            rightMinimumIndex += 1

            if rightMinimumIndex > arrayEnd:
                break

            rightMinimum = array[rightMinimumIndex]
            index = lastSwapIndex + 1
            # index = 0
            # print(f"roll over to {index}")
            lastSwapIndex = -1

        progress(index)

    print(f"lookups: {lookups}; swaps: {swaps}")
