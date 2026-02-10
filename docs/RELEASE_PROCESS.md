These are the steps to follow when creating a new release of the HDF5 Async VOL connector:

0. Ensure all changes are ready for the release and committed to the repository

1. Update the `VERSION` number specified for the `project ()` command in CMakeLists.txt in the root of the source tree with the new version number

    <b>Example:</b>
    ```CMake
    project(HDF5_VOL_ASYNC VERSION 1.9.0 LANGUAGES C)
    ```

    becomes

    ```CMake
    project(HDF5_VOL_ASYNC VERSION 2.0.0 LANGUAGES C)
    ```

2. Commit the change to the CMakeLists.txt file, using the version number with a leading "v" as the commit message

    <b>Example:</b>
    ```bash
    git add CMakeLists.txt
    git commit -m "v2.0.0"
    git push
    ```

3. Create a tag pointing to the commit from the previous step, using a leading "v" for the tag name

    <b>Example with signed tag:</b>
    ```bash
    git tag -s v2.0.0 -m "HDF5 Async VOL 2.0.0"
    ```

    <b>Example with unsigned tag:</b>
    ```bash
    git tag -a v2.0.0 -m "HDF5 Async VOL 2.0.0"
    ```

4. Push the tag from the previous step, triggering the release workflow

    <b>Example:</b>
    ```bash
    git push origin v2.0.0
    ```

5. On GitHub, edit the draft release created by the release workflow to tidy up any details, then publish the release when finished
