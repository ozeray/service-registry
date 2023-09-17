package com.ahmet.management;

public interface OnElectionCallback {

    void onElectedToBeLeader();
    void onElectedAsWorker();

}
