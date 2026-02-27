add_library_unity(test_sort OBJECT test_kway_merge.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sort>
    PARENT_SCOPE)
