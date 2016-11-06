package master2016;

class AvailableRoom {
    private static final int N_ROOMS = 2;

    static int getRandomRoom() {
        return (int) (Math.random() * AvailableRoom.N_ROOMS);
    }
}
